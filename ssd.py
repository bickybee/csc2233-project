import pandas
import operator
import sys

DEFAULT_TRACE_PATH = 'data/workloada_csv.csv'

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

def compute_sector_partitions(trace):
    # get and sort sector write counts
    sector_counts = compute_sector_write_counts(trace)
    sorted_counts = sorted(sector_counts.items(), key=operator.itemgetter(1), reverse=True) # returns list of tuples
    # init vars for partition generation loop
    max_count = sorted_counts[0][1]
    current_max = max_count
    dead_sectors_index = -1
    partitions = [[]]
    # go through sectors from highest to lowest counts, creating partitions
    for i, tup in enumerate(sorted_counts):
        count = tup[1]
        # if we hit the dead sectors, stop here
        if (count == 0):
            dead_sectors_index = i
            break
        # if we hit the max desired ratio, create a new partition
        if (current_max / count > 2):
            current_max = count
            partitions.append([])
        partitions[-1].append(tup)
    # give dead sectors their own partition
    dead_sectors = sorted_counts[dead_sectors_index:]
    partitions.append(dead_sectors)
    
    # print some stuff to console to check it's working...
    print("number of partitions: " + str(len(partitions)))
    size_str = "partition sizes: "
    for p in partitions:
        size_str += str(len(p)) + ", "
    print(size_str)

if __name__ == "__main__":
    trace_path = DEFAULT_TRACE_PATH
    if (len(sys.argv) == 2):
        trace_path = sys.argv[1]
    trace = pandas.read_csv(trace_path)
    compute_sector_partitions(trace)