# SUPER INCOMPLETE did not end up being used!

import math
from enum import Enum
from traceUtils import find_completed_writes

class state(Enum):
    ERASED = 0
    VALID = 1
    INVALID = 2

class TempPartitionMap:
    def __init__(self, partitions):
        self.map = {}
        for i, partition in enumerate(partitions):
            for page in partition:
                self.map[page[0]] = i

    def get_partition(self, page_address):
        return self.map[page_address]

class DeathPartitionMap:
    def __init__(self, partitions):
        self.map = {}
        for i, partition in enumerate(partitions):
            for page in partition:
                if self.map[page[0]] is None:
                    self.map[page[0]] = []
                self.map[page[0]].append(i)

    def get_partition(self, address):
        return self.map[page[0]].pop(0)



class SSDSimulator:
    def __init__(self, partitions, partition_type, trace, pages_per_block):
        if partition_type == 0:
            self.partition_map = new TempPartitionMap(partitions)
        else:
            self.partition_map = new DeathPartitionMap(partitions)
        self.ssd = []
        self.active_pages = []
        for partition_pages in partitions:
            num_blocks = math.ceil(len(partition_pages) / pages_per_block)
            partition_blocks = [[state.ERASED for i in range(pages_per_block)] for j in range(num_blocks)]
            self.ssd.append(partition_blocks)
            self.active_pages.append(0)
        self.writes = find_completed_writes(trace)

    def write_to_partition(self, page, partition_num):
        print("boop")

    def run_trace(self):
        for i, write in self.writes.iterrows():
        starting_address = write['sector_number']
        ending_address = starting_address + write['request_size']
        aligned_starting_address = math.floor(starting_address / page_size) * page_size
        for page in range(aligned_starting_address, ending_address + 1, page_size):
            which_partition = self.partition_map.get_partition(page)
            self.write_to_partition(page, which_partition)

