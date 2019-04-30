import pandas
import math
import operator
from traceUtils import find_completed_writes, compute_max_sector_number

PAGE_SIZE = 8

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
    print(partitions[0])



if __name__ == "__main__":
    trace = pandas.read_csv('../data/formatted/workloada_trace_btrfs.csv')
    partition_by_death_times(trace, 4)

