import pandas
import math
import os
import operator
import statistics
from partitionExperiment import create_partitions, compute_minimum_frequency
from traceUtils import find_completed_writes, compute_max_sector_number, sorted_page_counts

PAGE_SIZE = 8
BLOCK_SIZE = 512
PAGES_PER_BLOCK = BLOCK_SIZE / PAGE_SIZE
TRACE_FOLDER_PATH = '../data/formatted/'

def find_page_write_times(trace, page_size):
    writes = find_completed_writes(trace)
    num_sectors = compute_max_sector_number(writes)
    writes.sort_values('timestamp')
    # initialize write time lists as empty
    # each element in the dict corresponds to a page
    page_write_times = {page_address: [] for page_address in range(0, num_sectors + 1, page_size)}
    # for each page, append times at which it was written to
    for i, write in writes.iterrows():
        starting_address = write['sector_number']
        ending_address = starting_address + write['request_size']
        aligned_starting_address = math.floor(starting_address / page_size) * page_size
        for page in range(aligned_starting_address, ending_address + 1, page_size):
            page_write_times[page].append(write['timestamp'])

    return page_write_times

def sort_by_death_time(trace):
    write_times = find_page_write_times(trace, PAGE_SIZE)
    # deaths = all writes to a page AFTER the first write
    death_times = [[(address, t) for t in times] for address, times in write_times.items() if len(times) > 1]
    # flatten list...
    death_times = [tup for l in death_times for tup in l]
    # sort!
    death_times = sorted(death_times, key=operator.itemgetter(1))
    # print(death_times)

    return death_times

def partition_by_death_times(trace, num_partitions):
    death_times = sort_by_death_time(trace)

    # find intervals by shifting and subtracting
    times0 = death_times[:-1]
    times1 = death_times[1:]
    time_intervals = [t1[1] - t0[1] for t1, t0 in zip(times1, times0)]

    # create boundaries at largest intervals
    biggest_intervals = sorted(enumerate(time_intervals), key=operator.itemgetter(1), reverse=True)[:num_partitions]
    boundary_indices = [tup[0] for tup in biggest_intervals]
    boundary_indices = sorted(boundary_indices)
    boundary_indices.insert(0, 0)
    boundary_indices.append(len(death_times) - 1)
    # partition!
    partitions = [death_times[boundary_indices[i] : boundary_indices[i + 1]] for i in range(len(boundary_indices) - 1)]

def compute_partition_death_time_deviations(trace, partitions):
    print('boop')

# page_map[address] = (partition_index, [death_time_0,...,death_time_n])
def create_page_map(trace, partitions):
    # trace_end_time = 5000
    all_write_times = find_page_write_times(trace, PAGE_SIZE)
    page_map = {}

    for i, partition in enumerate(partitions):
        for page in partition:
            address = page[0]
            death_times = []

            if address in all_write_times:
                write_times = all_write_times[address]
                if len(write_times) > 1:
                    death_times = write_times[1:]

            page_map[address] = (i, death_times)
    
    return page_map


def death_time_deviation_experiment(trace):

    max_time = find_completed_writes(trace)['timestamp'].max()
    default_time = max_time # ARBITRARY CHOICE OH WELL

    # really need to refactor this prep into the functions themselves lol
    sorted_counts = sorted_page_counts(trace, PAGE_SIZE)
    max_count = sorted_counts[0][1]
    partitions = create_partitions(sorted_counts, max_count, PAGE_SIZE, 2)
    page_map = create_page_map(trace, partitions)
    writes = find_completed_writes(trace)

    # ssd => list of partitions => list of blocks
    # INFINITE SPACE (-: no GC!
    ssd_partitioned = [[[]] for i in range(len(partitions))]
    ssd_non_partitioned = [[]]
    # print(ssd)

    # write death times to ssd blocks
    for i, write in writes.iterrows():

        # align write to page
        starting_address = write['sector_number']
        ending_address = starting_address + write['request_size']
        aligned_starting_address = math.floor(starting_address / PAGE_SIZE) * PAGE_SIZE

        # "write" to partition, active block 
        for page_address in range(aligned_starting_address, ending_address + 1, PAGE_SIZE):
            page_data = page_map[page_address]
            partition_num = page_data[0]
            death_times = page_data[1]

            # find next death of this page
            next_death = default_time # SHOULD HAVE DIFF DEFAULT
            if len(death_times) > 0:
                next_death = death_times.pop(0)

            # check if active block is full
            active_block = ssd_partitioned[partition_num][-1]
            if len(active_block) >= PAGES_PER_BLOCK:
                ssd_partitioned[partition_num].append([])  

            active_block = ssd_non_partitioned[-1]
            if len(active_block) >= PAGES_PER_BLOCK:
                ssd_non_partitioned.append([])
            
            # write the death time in!
            ssd_partitioned[partition_num][-1].append(next_death)
            ssd_non_partitioned[-1].append(next_death)

    # now check similarity btwn death times per block, per partition
    stdevs_p = []
    stdevs_np = []
    avg_stdevs = []
    for i, p in enumerate(ssd_partitioned):
        # print("partition %d:" % i)
        # stdevs_p.append([])
        for b in p:
            stdevs_p.append(statistics.stdev(b))
        # print("max: %d" % max(stdevs[i]))
        # print("medians: %d" % statistics.median(stdevs[i]))
        # avg_stdevs.append(round(statistics.mean(stdevs_p[i])))
    print("partitioned avg: %d" % statistics.mean(stdevs_p))

    for b in ssd_non_partitioned:
        stdevs_np.append(statistics.stdev(b))
    
    print("nonpartitioned avg: %d" % statistics.mean(stdevs_np))


if __name__ == "__main__":
    # trace = pandas.read_csv('../data/formatted/workloada_trace_f2fs.csv')
    # death_time_deviation_experiment(trace)
    results = {}

    for entry in os.scandir(TRACE_FOLDER_PATH):
        print(entry.name)
        trace = pandas.read_csv(TRACE_FOLDER_PATH + entry.name)
        avg_stdevs = death_time_deviation_experiment(trace)
        results[entry.name] = avg_stdevs
        # df = pandas.DataFrame(results)
        # df.to_csv('results6.csv')

