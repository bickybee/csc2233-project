import pandas
import operator
import sys
import math
import os
from enum import Enum

DEFAULT_TRACE_FOLDER_PATH = './data/formatted/'
DEFAULT_TRACE_PATH = 'data/formatted/workloada_trace_f2fs.csv'
DEFAULT_TARGET_RATIO = 2.0
THRESHOLD = 0.01
MAX_ITERATIONS = 20

class page_sizes(Enum):
    DEFAULT = 1
    ONE_KB = 2
    TWO_KB = 4
    FOUR_KB = 8
    EIGHT_KB = 16
    SIXTEEN_KB = 32
    THIRTY_TWO_KB = 64
    SIXTY_FOUR_KB = 128

EXPERIMENT_PARTITION_NUMS = [2, 4, 8, 12, 16, 24]
EXPERIMENT_PAGE_SIZES = [page_sizes.DEFAULT, page_sizes.FOUR_KB, page_sizes.SIXTEEN_KB, page_sizes.SIXTY_FOUR_KB]

def find_completed_writes(trace):
    writes = trace.loc[trace['operation'].str.contains('W')]
    completed_writes = writes.loc[writes['action'] == 'C']
    return completed_writes

def compute_max_sector_number(trace):
    return trace[['sector_number','request_size']].sum(axis=1).sort_values(ascending=False).head(1).iat[0]

def compute_page_write_counts(trace, page_size):
    writes = find_completed_writes(trace)
    num_sectors = compute_max_sector_number(writes)
    # initialize page write counts to 0
    # each element in the dict corresponds to a page
    # (a list might be faster but this way we can sort by count later and keep the page addresses)
    page_write_counts = {page_address: 0 for page_address in range(0, num_sectors + 1, page_size)}
    # compute write count for every page
    for i, write in writes.iterrows():
        starting_address = write['sector_number']
        ending_address = starting_address + write['request_size']
        aligned_starting_address = math.floor(starting_address / page_size) * page_size
        for page in range(aligned_starting_address, ending_address + 1, page_size):
            page_write_counts[page] += 1

    return page_write_counts

# greedy partitioning scheme!
def create_partitions(sorted_counts, max_count, page_size, target_ratio):
    current_max = max_count
    dead_pages_index = -1
    partitions = [[]]

    # go through page-size chunks of sectors from highest to lowest counts, creating partitions
    for i, (page_address, count) in enumerate(sorted_counts):

        # if we hit a dead page, stop here
        if (count == 0):
            dead_pages_index = i
            break

        # if we hit the max desired ratio, create a new partition
        if (current_max / count > target_ratio):
            current_max = count
            partitions.append([])
        
        # add all sectors from this page to the current partition
        partitions[-1].append((page_address, count))

    # give dead sectors their own partition
    dead_pages = sorted_counts[dead_pages_index:]
    partitions.append(dead_pages)

    return partitions

def compute_ideal_partitions(sorted_counts, page_size):
    # get and sort sector write counts
    page_counts = compute_page_write_counts(trace, page_size)
    sorted_counts = sorted(page_counts.items(), key=operator.itemgetter(1), reverse=True) # returns list of tuples

    # init vars for partition generation loop
    max_count = sorted_counts[0][1]

    partitions = create_partitions(sorted_counts, max_count, page_size, DEFAULT_TARGET_RATIO)
    num_partitions = len(partitions)

    return num_partitions

# exhaustively find fmin(N) — the lowest ratio for which the greedy partitioning scheme results in max_num_partitions
def compute_minimum_frequency(sorted_counts, page_size, max_num_partitions):
    # get and sort sector write counts
    page_counts = compute_page_write_counts(trace, page_size)
    sorted_counts = sorted(page_counts.items(), key=operator.itemgetter(1), reverse=True) # returns list of tuples

    # init vars for partition generation loop
    max_count = sorted_counts[0][1]
    lower_bound = 1.0
    upper_bound = None
    best_ratio = None
    delta = math.inf
    target_ratio = DEFAULT_TARGET_RATIO
    num_partitions = math.inf
    iterations_taken = 0

    # exhaustive search, go!
    for i in range(MAX_ITERATIONS): # to make sure we don't go forever :-)

        iterations_taken = i + 1

        # compute partitions for current target ratio
        partitions = create_partitions(sorted_counts, max_count, page_size, target_ratio)
        num_partitions = len(partitions)
        new_target = None

        # if we still have too many partitions, increase target ratio
        if (num_partitions > max_num_partitions):
            lower_bound = target_ratio
            if upper_bound is not None:
                new_target = target_ratio + ((upper_bound - target_ratio) / 2.0)
            else:
                new_target = target_ratio * 2.0
            delta = new_target - target_ratio

        # if we're within the right number of partitions, search for best ratio
        else:
            best_ratio = target_ratio # save this as our best candidate so far!
            upper_bound = target_ratio
            new_target = target_ratio - ((target_ratio - lower_bound) / 2.0)
            delta = target_ratio - new_target

        # if we're barely moving, break    
        if delta <= THRESHOLD:
            break

        target_ratio = new_target

    return best_ratio

def compute_spatial_locality_probability(trace):
    t=5000
    d=1024

    # filter out only writes
    write_trace = find_completed_writes(trace)

    # virtual time; a counter incemented for each IO request which is inferred to be the sequence number
    # filter out columns we want
    #filtered_trace = write_trace[['timestamp', 'sector_number', 'request_size']]
    filtered_trace = write_trace[['sequence_number', 'sector_number', 'request_size']]

    # sort trace by descending timestamp
    time_trace = filtered_trace.sort_values(by=['sequence_number'], ascending=False)

    # shift trace upwards to compare values
    time_trace_shift = time_trace.shift(-1)
    time_trace['hit'] = ((time_trace['sequence_number'] - time_trace_shift['sequence_number']) <= t) \
                        & (abs(time_trace['sector_number'] - time_trace_shift['sector_number']) <= d)

    # we do not consider pages in the same requests as hits; they are ignored. We only look at separate requests
    hits = time_trace['hit'].sum()

    # we ignore the last page, since it has no other page to compare it to
    probability = hits / (len(time_trace['hit']) - 1)

    print("Spatial locality probability: ")
    print("t: %d d: %d probability %s" % (t, d, probability))

# compute sector partitions for all traces
def run_partition_experiment_1(traces_dict):
    print("EXPERIMENT 1")
    results = {}

    for trace_name, trace in traces_dict.items():
        print(trace_name)
        results[trace_name] = compute_ideal_partitions(trace, page_sizes.DEFAULT.value)

    return results

# compute best ratio given a max partitioning N for all traces
def run_partition_experiment_2(traces_dict):
    results = {}

    for partition_num in EXPERIMENT_PARTITION_NUMS:
        print(str(partition_num) + " partitions")
        results[partition_num] = []
        for trace_name, trace in traces_dict.items():
            print(trace_name)
            result = compute_minimum_frequency(trace, page_sizes.DEFAULT.value, partition_num)
            results[partition_num].append(result)

    return results

# compute best ratio given a max partitioning N and page size for all traces
def run_partition_experiment_3(traces_dict):
    results = {}
    result_avgs = {}

    for partition_num in EXPERIMENT_PARTITION_NUMS:
        result_avgs[partition_num] = {}
        for size in EXPERIMENT_PAGE_SIZES:
            key = str(partition_num) + "," + str(size.name)
            results[key] = []
            result_avgs[partition_num][size.name] = 0
            print(key)
            for trace_name, trace in traces_dict.items():
                print(trace_name)
                result = compute_minimum_frequency(trace, size.value, partition_num)
                results[key].append(result)
                result_avgs[partition_num][size] += result
            result_avgs[partition_num][size] /= len(results[key])
            print(result_avgs[partition_num][size])
        print(result_avgs)

    return results, result_avgs

# run all 3 partitioning experiments and output results as csv files
def run_partition_experiments(trace_folder_path):
    traces = {}

    for entry in os.scandir(trace_folder_path):
        trace = pandas.read_csv(folder_path + entry.name)
        traces[entry.name] = trace

    results1 = run_partition_experiment_1(traces)
    df1 = pandas.DataFrame(results1, index=[0])
    df1.to_csv('results1.csv')

    results2 = run_partition_experiment_2(traces)
    df2 = pandas.DataFrame(results2)
    df2.to_csv('results2.csv')

    results3, results3avgs = run_partition_experiment_3(traces)
    df3 = pandas.DataFrame(results3)
    df3.to_csv('results3_take2.csv')
    df3avgs = pandas.DataFrame(results3avgs)
    df3avgs.to_csv('results3avgs.csv')


if __name__ == "__main__":
    folder_path = DEFAULT_TRACE_FOLDER_PATH
    trace_path = DEFAULT_TRACE_PATH

    if (len(sys.argv) == 2):
        trace_path = sys.argv[1]

    trace = pandas.read_csv(trace_path)
    # compute_spatial_locality_probability(trace)

    run_partition_experiments(folder_path)
    
