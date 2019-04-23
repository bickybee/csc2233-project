import pandas
import operator
import sys
import math
import os
from enum import Enum

DEFAULT_TRACE_FOLDER_PATH = './data/formatted/'
DEFAULT_OUTPUT_PATH = 'output.csv'
DEFAULT_TARGET_RATIO = 2
THRESHOLD = 0.01
MAX_ITERATIONS = 20

class page_size(Enum):
    DEFAULT = 1
    ONE_KB = 2
    TWO_KB = 4
    FOUR_KB = 8
    EIGHT_KB = 16
    SIXTEEN_KB = 32
    THIRTY_TWO_KB = 64
    SIXTY_FOUR_KB = 128

EXPERIMENT_PARTITION_NUMS = [2, 4, 8, 12, 16, 24]
EXPERIMENT_PAGE_SIZES = [page_size.DEFAULT, page_size.FOUR_KB, page_size.SIXTEEN_KB, page_size.SIXTY_FOUR_KB]

def find_completed_writes(trace):
    writes = trace.loc[trace['operation'].str.contains('W')]
    completed_writes = writes.loc[writes['action'] == 'C']
    return completed_writes

def compute_max_sector_number(trace):
    return trace[['sector_number','request_size']].sum(axis=1).sort_values(ascending=False).head(1).iat[0]

def compute_sector_write_counts(trace):
    writes = find_completed_writes(trace)
    num_sectors = compute_max_sector_number(writes)
    # initialize sector write counts to 0
    # each element in the dict corresponds to a sector
    # (a list might be faster but this way we can sort by count later and keep the sector numbers)
    sector_write_counts = {sector_number: 0 for sector_number in range(num_sectors + 1)}
    # compute write count for every sector
    for i, write in writes.iterrows():
        starting_sector = write['sector_number']
        ending_sector = starting_sector + write['request_size']
        for sector in range(starting_sector, ending_sector + 1):
            sector_write_counts[sector] += 1

    return sector_write_counts

# greedy partitioning scheme!
def create_partitions(sorted_counts, max_count, page_size, target_ratio):
    current_max = max_count
    dead_sectors_index = -1
    partitions = [[]]

    # go through page-size chunks of sectors from highest to lowest counts, creating partitions
    for i in range(0, len(sorted_counts), page_size):

        page = sorted_counts[i : i + page_size] 

        # calculates the count for the page
        count = 0
        for sector in page:
            count += sector[1]

        # if we hit a dead page, stop here
        if (count == 0):
            dead_sectors_index = i
            break

        # if we hit the max desired ratio, create a new partition
        if (current_max / count > target_ratio):
            current_max = count
            partitions.append([])
        
        # add all sectors from this page to the current partition
        partitions[-1].extend(page)

    # give dead sectors their own partition
    dead_sectors = sorted_counts[dead_sectors_index:]
    partitions.append(dead_sectors)

    return partitions

def compute_sector_partitions(trace, page_size):
    # get and sort sector write counts
    sector_counts = compute_sector_write_counts(trace)
    sorted_counts = sorted(sector_counts.items(), key=operator.itemgetter(1), reverse=True) # returns list of tuples

    print(sorted_counts[:50])

    # init vars for partition generation loop
    max_count = 0
    for i in range(page_size):
        max_count += sorted_counts[i][1]

    partitions = create_partitions(sorted_counts, max_count, page_size, DEFAULT_TARGET_RATIO)
    
    # print some stuff to console to check it's working...
    num_partitions = len(partitions)
    # print("number of partitions: " + str(num_partitions))
    # size_str = "partition sizes: "
    # for p in partitions:
    #     size_str += str(len(p)) + ", "
    # print(size_str)

    return num_partitions

# exhaustively find fmin(N) — the lowest ratio for which the greedy partitioning scheme results in max_num_partitions
def compute_minimum_frequency(trace, page_size, max_num_partitions, min_target_ratio, threshold, max_iterations):
    # get and sort sector write counts
    sector_counts = compute_sector_write_counts(trace)
    sorted_counts = sorted(sector_counts.items(), key=operator.itemgetter(1), reverse=True) # returns list of tuples

    # init vars for partition generation loop
    max_count = 0
    for i in range(page_size):
        max_count += sorted_counts[i][1]
    lower_bound = 1
    upper_bound = None
    best_ratio = None
    delta = math.inf
    target_ratio = min_target_ratio
    num_partitions = math.inf
    iterations_taken = 0

    # exhaustive search, go!
    for i in range(max_iterations): # to make sure we don't go forever :-)

        iterations_taken = i + 1
        # print(target_ratio)

        # compute partitions for current target ratio
        partitions = create_partitions(sorted_counts, max_count, page_size, target_ratio)
        num_partitions = len(partitions)
        new_target = None

        # if we still have too many partitions, increase target ratio
        if (num_partitions > max_num_partitions):
            # print("increase")
            lower_bound = target_ratio
            if upper_bound is not None:
                new_target = target_ratio + ((upper_bound - target_ratio) / 2.0)
            else:
                new_target = target_ratio * 2
            delta = new_target - target_ratio

        # if we're within the right number of partitions, search for best ratio
        else:
            # print("decrease")
            best_ratio = target_ratio # save this as our best candidate so far!
            upper_bound = target_ratio
            new_target = target_ratio - ((target_ratio - lower_bound) / 2.0)
            delta = target_ratio - new_target

        # if we're barely moving, break    
        if delta <= threshold:
                break

        target_ratio = new_target

    # print("fmin(N): ")
    # print(best_ratio)

    # print("final delta: ")
    # print(delta)

    # print("iterations taken")
    # print(iterations_taken)

    return best_ratio

def run_experiment_1(traces_dict):
    print("EXPERIMENT 1")
    results = {}

    for trace_name, trace in traces_dict.items():
        print(trace_name)
        results[trace_name] = compute_sector_partitions(trace, page_size.DEFAULT.value)

    return results

def run_experiment_2(traces_dict):
    results = {}

    for partition_num in EXPERIMENT_PARTITION_NUMS:
        print(str(partition_num) + " partitions")
        results[partition_num] = []
        for trace_name, trace in traces_dict.items():
            print(trace_name)
            result = compute_minimum_frequency(trace, page_size.DEFAULT.value, partition_num, DEFAULT_TARGET_RATIO, THRESHOLD, MAX_ITERATIONS)
            results[partition_num].append(result)

    return results

def run_experiment_3(traces_dict):
    results = {}

    for partition_num in EXPERIMENT_PARTITION_NUMS:
            for size in EXPERIMENT_PAGE_SIZES:
                key = str(partition_num) + "," + str(size.name)
                results[key] = []
                print(key)
                for trace_name, trace in traces_dict.items():
                    print(trace_name)
                    result = compute_minimum_frequency(trace, size.value, partition_num, DEFAULT_TARGET_RATIO, THRESHOLD, MAX_ITERATIONS)
                    results[key].append(result)
    
    return results

if __name__ == "__main__":
    folder_path = DEFAULT_TRACE_FOLDER_PATH
    traces = {}

    if (len(sys.argv) == 2):
        folder_path = sys.argv[1]

    for entry in os.scandir(folder_path):
        trace = pandas.read_csv(folder_path + entry.name)
        traces[entry.name] = trace

    results1 = run_experiment_1(traces)
    df1 = pandas.DataFrame(results1, index=[0])
    df1.to_csv('results1.csv')

    results2 = run_experiment_2(traces)
    df2 = pandas.DataFrame(results2)
    df2.to_csv('results2.csv')

    results3 = run_experiment_3(traces)
    df3 = pandas.DataFrame(results3)
    df3.to_csv('results3.csv')
    
