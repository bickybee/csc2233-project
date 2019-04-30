import pandas
import operator
import sys
import math
import os
from enum import Enum

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