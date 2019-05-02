import pandas
import operator
import sys
import math
from enum import Enum

# Note: it might take a while to run!


DEFAULT_TRACE_PATH = '../data/formatted/workloadf_trace_f2fs.csv'


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


def compute_spatial_locality_probability(trace, page_size):

    # filter out only writes
    write_trace = find_completed_writes(trace)

    # virtual time; a counter incremented for each IO request which is inferred to be the sequence number
    #filtering out columns we want
    filtered_trace = write_trace[['sequence_number', 'sector_number', 'request_size']]

    # sort trace by descending sequence_number
    time_trace = filtered_trace.sort_values(by=['sequence_number'], ascending=False)

    probability_list = []
    hit_number = 0
    hits = []

    print("Calculating logical locality for: " + DEFAULT_TRACE_PATH)
    t = 0
    for i in range(9):
        d = 0
        for j in range(9):
            hit_number = 0
            for row in time_trace.itertuples():
                #time_hit = False
                #distance_hit = False

                # checking if there are any values within the time window (not including itself)
                time_trace['time_hit'] = time_trace['sequence_number'].between(row.sequence_number-t-1, row.sequence_number-1)
                time_hits = time_trace.loc[time_trace['time_hit'] == True]

                # if above 0, we have a time locality hit
                if((time_trace['time_hit'].sum() > 0)):
                    #time_hit = True

                    # checking for spatial locality
                    hits_above = time_hits['sector_number'].between(row.sector_number, row.sector_number + (d * page_size))
                    hits_below = time_hits['sector_number'].between(row.sector_number - (d * page_size) + row.request_size, row.sector_number)

                    if(hits_above.sum() or hits_below.sum() > 0):
                        hit_number += 1
                        #distance_hit = True

                #hits.append(time_hit and distance_hit)

            # hits.append(time_hit and distance_hit)
            probability = hit_number / len(time_trace['sequence_number'] - 1)
            row = {"Time": t, "Distance": d, "Probability": probability}
            print(row)
            probability_list.append(row.values())
            d += 32
        t += 250

    #time_trace['hit'] = hits
    # print(probability_list)

    df = pandas.DataFrame(probability_list, columns=['Time', 'Distance','Probability'])
    #df.to_csv('C:/Users/scott/PycharmProjects/graph_locality/graph_locality_data/test6.csv')
    print(df)
    return df

if __name__ == "__main__":
    trace_path = DEFAULT_TRACE_PATH
    page_size = page_size.FOUR_KB.value

    trace = pandas.read_csv(trace_path)
    df = compute_spatial_locality_probability(trace, page_size)