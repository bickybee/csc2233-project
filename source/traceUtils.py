import pandas
import operator
import math
import statistics
import os
import sys

TRACE_FOLDER_PATH = '../data/formatted/'
PAGE_SIZE = 8

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

def sorted_page_counts(trace, page_size):
    counts = compute_page_write_counts(trace, page_size)
    sorted_counts = sorted(counts.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_counts

def write_stats(trace, page_size):
    writes = find_completed_writes(trace)
    num_writes = len(writes)
    
    write_counts = compute_page_write_counts(trace, page_size).values()
    write_counts = [c for c in write_counts if c > 0]
    max_count = max(write_counts)
    median_count = statistics.median(write_counts)
    max_update_freq = max_count / num_writes

    print("total: %d, max: %d, max freq: %f" % (num_writes, max_count, max_update_freq))

    return num_writes, max_count, max_update_freq

if __name__ == "__main__":
    # trace = pandas.read_csv('../data/formatted/workloada_trace_f2fs.csv')
    # death_time_deviation_experiment(trace)
    results = {}

    for entry in os.scandir(TRACE_FOLDER_PATH):
        print(entry.name)
        trace = pandas.read_csv(TRACE_FOLDER_PATH + entry.name)
        write_stats(trace, PAGE_SIZE)
        # df = pandas.DataFrame(results)
        # df.to_csv('results6.csv')