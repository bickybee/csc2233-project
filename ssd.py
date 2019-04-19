import pandas
import operator
import sys
import math
from enum import Enum

DEFAULT_TRACE_PATH = 'data/workloada_csv.csv'
DEFAULT_TARGET_RATIO = 2

class page_size(Enum):
    DEFAULT = 1
    ONE_KB = 2
    TWO_KB = 4
    FOUR_KB = 8
    EIGHT_KB = 16
    SIXTEEN_KB = 32
    THIRTY_TWO_KB = 64
    SIXTY_FOUR_KB = 128

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

def create_partitions(sorted_counts, max_count, page_size, target_ratio):
    current_max = max_count
    dead_sectors_index = -1
    partitions = [[]]
    ratios = []

    prev_count = max_count

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

    # init vars for partition generation loop
    max_count = 0
    for i in range(page_size):
        max_count += sorted_counts[i][1]

    partitions = create_partitions(sorted_counts, max_count, page_size, DEFAULT_TARGET_RATIO)
    
    # print some stuff to console to check it's working...
    print("number of partitions: " + str(len(partitions)))
    size_str = "partition sizes: "
    for p in partitions:
        size_str += str(len(p)) + ", "
    print(size_str)


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
    target_ratio = min_target_ratio # starting point
    num_partitions = math.inf
    iterations_taken = 0

    for i in range(max_iterations): # to make sure we don't go forever :-)
        iterations_taken = i + 1
        print(target_ratio)
        partitions = create_partitions(sorted_counts, max_count, page_size, target_ratio)
        num_partitions = len(partitions)
        new_target = None
        # if we still have too many partitions, increase target ratio
        if (num_partitions > max_num_partitions):
            print("increase")
            lower_bound = target_ratio
            if upper_bound is not None:
                new_target = target_ratio + ((upper_bound - target_ratio) / 2.0)
            else:
                new_target = target_ratio * 2
            delta = new_target - target_ratio
        # if we're within the right number of partitions, search for best ratio
        else:
            print("decrease")
            best_ratio = target_ratio
            upper_bound = target_ratio
            new_target = target_ratio - ((target_ratio - lower_bound) / 2.0)
            delta = target_ratio - new_target
        # if we're barely moving, break    
        if delta <= threshold:
                break
        target_ratio = new_target


    print("final ratio: ")
    print(target_ratio)

    print("final partitions: ")
    print(num_partitions)

    print("best ratio: ")
    print(best_ratio)

    print("final delta: ")
    print(delta)

    print("iterations taken")
    print(iterations_taken)

    return target_ratio # should compute and return real ratio!

if __name__ == "__main__":
    trace_path = DEFAULT_TRACE_PATH
    page_size = page_size.DEFAULT.value

    max_num_partitions = 3.0
    initial_target_ratio = 2.0
    threshold = 0.1
    max_iterations = 10

    if (len(sys.argv) == 2):
        trace_path = sys.argv[1]

    trace = pandas.read_csv(trace_path)
    # compute_sector_partitions(trace, page_size)
    compute_minimum_frequency(trace, page_size, max_num_partitions, initial_target_ratio, threshold, max_iterations)
