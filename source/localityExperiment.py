import pandas
import sys
import math
import os
from traceUtils import find_completed_writes

DEFAULT_TRACE_PATH = '../data/formatted/workloada_trace_f2fs.csv'

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

if __name__ == "__main__":
    trace_path = DEFAULT_TRACE_PATH

    if (len(sys.argv) == 2):
        trace_path = sys.argv[1]

    trace = pandas.read_csv(trace_path)
    compute_spatial_locality_probability(trace)
    