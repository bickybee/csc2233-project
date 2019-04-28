import pandas
import operator
import sys
import math
import os
from enum import Enum
from traceUtils import find_completed_writes, compute_max_sector_number, compute_page_write_counts

DEFAULT_TRACE_FOLDER_PATH = '../data/formatted/'
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
    # init vars for partition generation loop
    max_count = sorted_counts[0][1]

    partitions = create_partitions(sorted_counts, max_count, page_size, DEFAULT_TARGET_RATIO)
    num_partitions = len(partitions)

    return num_partitions

# exhaustively find fmin(N) — the lowest ratio for which the greedy partitioning scheme results in max_num_partitions
def compute_minimum_frequency(sorted_counts, page_size, max_num_partitions):

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

# compute sector partitions for all traces
def run_partition_experiment_1(traces_dict):
    print("EXPERIMENT 1")
    results = {}

    for trace_name, trace in traces_dict.items():
        print(trace_name)
         # get and sort sector write counts
        page_counts = compute_page_write_counts(trace, page_sizes.DEFAULT.value)
        sorted_counts = sorted(page_counts.items(), key=operator.itemgetter(1), reverse=True) # returns list of tuples
        results[trace_name] = compute_ideal_partitions(sorted_counts, page_sizes.DEFAULT.value)

    return results

# compute best ratio given a max partitioning N for all traces
def run_partition_experiment_2(traces_dict):
    results = dict.fromkeys(EXPERIMENT_PARTITION_NUMS, [])

    for trace_name, trace in traces_dict.items():
        print(trace_name)

        page_counts = compute_page_write_counts(trace, page_sizes.DEFAULT.value)
        sorted_counts = sorted(page_counts.items(), key=operator.itemgetter(1), reverse=True)

        for partition_num in EXPERIMENT_PARTITION_NUMS:
            print(str(partition_num) + " partitions")

            result = compute_minimum_frequency(sorted_counts, page_sizes.DEFAULT.value, partition_num)
            results[partition_num].append(result)

    return results

# compute best ratio given a max partitioning N and page size for all traces
def run_partition_experiment_3(traces_dict):
    keys = []

    for partition_num in EXPERIMENT_PARTITION_NUMS:
        for size in EXPERIMENT_PAGE_SIZES:
            keys.append(str(partition_num) + "," + str(size.name))

    # EXCUSE THIS UGLYNESS!
    results = dict((k, []) for k in keys)
    result_sums = dict((k1, dict((k2, 0) for k2 in [size.name for size in EXPERIMENT_PAGE_SIZES])) for k1 in EXPERIMENT_PARTITION_NUMS)
    result_avgs = dict((k1, dict((k2, 0) for k2 in [size.name for size in EXPERIMENT_PAGE_SIZES])) for k1 in EXPERIMENT_PARTITION_NUMS)

    for trace_name, trace in traces_dict.items():
        print(trace_name)

        for size in EXPERIMENT_PAGE_SIZES:
            page_counts = compute_page_write_counts(trace, size.value)
            sorted_counts = sorted(page_counts.items(), key=operator.itemgetter(1), reverse=True)

            for partition_num in EXPERIMENT_PARTITION_NUMS:
                key = str(partition_num) + "," + str(size.name)

                result = compute_minimum_frequency(sorted_counts, size.value, partition_num)
                results[key].append(result)
                result_sums[partition_num][size.name] += result

                print(key)

    # get averages from results
    n = len(results[keys[0]])
    for k1, v1 in result_sums.items():
        for k2, v2 in v1.items():
            result_avgs[k1][k2] = v2 / n

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
    df3maxs = pandas.DataFrame(results3avgs)
    df3maxs.to_csv('results3maxs.csv')


if __name__ == "__main__":
    folder_path = DEFAULT_TRACE_FOLDER_PATH

    if (len(sys.argv) == 2):
        folder_path = sys.argv[1]

    run_partition_experiments(folder_path)
    
