import math
import os
import sys
import pandas
from partitionExperiment import create_partitions
from traceUtils import sorted_page_counts

DEFAULT_TRACE_FOLDER_PATH = '../data/formatted/'
PAGE_SIZE = 8
TARGET_RATIO = 2
MAX_N = 256

def equalize_partitions_loose(trace):
    sorted_counts = sorted_page_counts(trace, 8)
    max_count = sorted_counts[0][1]
    partitions = create_partitions(sorted_counts, max_count, PAGE_SIZE, TARGET_RATIO)

    temperature_sizes = [len(p) for p in partitions]
    temperature_sizes.sort(reverse=True)

    total_space = sum(temperature_sizes)
    min_N = len(partitions)
    max_N = MAX_N
    results = {}
    N = min_N
    div = 2
    i = 0
    partition_size = temperature_sizes[0] # start w biggest partition as size

    while (N <= max_N):
        # step 1)
        # calculate wasted space for N
        wasted_space = 0
        for size in temperature_sizes:
            wasted_space += (math.ceil(size / partition_size) * partition_size) - size
        percent_wasted_space = 100.0 * (wasted_space / total_space)
        results[N] = percent_wasted_space

        # step 2)
        # find new partition size
        partition_size = math.ceil(temperature_sizes[i] / div)
        if temperature_sizes[i + 1] <= partition_size:
            div += 1
        else:
            partition_size = temperature_sizes[i + 1]
            i = i + 1
            div = 2
        print("partition size: %d" % partition_size)

        # step 3)
        # find N for this partition size
        N = 0
        for size in temperature_sizes:
            divisions = math.ceil(size / partition_size)
            N += math.ceil(size / partition_size)
            print("%d divisions for size %d" % (divisions, size))

    return results

def equalize_partitions_tight(trace):
    results = {}

    all_page_counts = sorted_page_counts(trace, 8)
    counts = [page_count for page_count in all_page_counts if page_count[1] != 0]
    num_pages = len(counts)

    for n in range(2, MAX_N):
        partition_size = math.ceil(num_pages / n)
        partitions = [counts[i : i + partition_size] for i in range(0, num_pages, partition_size)]
        ratios = [p[0][1] / p[-1][1] for p in partitions]
        results[n] = max(ratios)

    return results


def run_experiment(trace_folder_path):

    results_loose = {}
    results_tight = {}

    for entry in os.scandir(trace_folder_path):
        trace = pandas.read_csv(folder_path + entry.name)
        loose_partitions = equalize_partitions_loose(trace)
        tight_partitions = equalize_partitions_tight(trace)
        results_loose[entry.name] = loose_partitions
        results_tight[entry.name] = tight_partitions

    df1 = pandas.DataFrame(results_loose)
    df1.to_csv('results4.csv')

    df2 = pandas.DataFrame(results_tight)
    df2.to_csv('results5.csv')
        
if __name__ == '__main__':

    folder_path = DEFAULT_TRACE_FOLDER_PATH

    if (len(sys.argv) == 2):
        folder_path = sys.argv[1]

    run_experiment(folder_path)
