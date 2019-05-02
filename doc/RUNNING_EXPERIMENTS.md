# Running Experiments

We created a variety of scripts to help run our experiments.
After setting up the pipenv environment, navigate to the source folder and run scripts using

```
pipenv run python script_name.py
```

For the following scripts, the trace source folder can be passed as a command-line argument, but defaults to our data/formatted folder. See the files in that folder for trace formatting reference-- we added column headers and removed excess data from the ends of the blkparse outputs.

- partitionExperiments.py
  - Outputs relate to the temperature partition and access granularity experiments
    - results1.csv: number of partitions per trace for fmin <= 2
    - results2.csv: fmin(N) per trace
    - results3.csv: fmin(N) per trace per page size, raw data
    - results3avgs.csv: averaged data for easier graphing in excel
- equalPartitions.py
  - Outputs relate to the equal partitioning experiments (excess page curves)
    - results4.csv: percent of excess pages for equal partition sizes (as discussed in the paper)
    - results5.csv: frequency ratio for just splitting up partitions equally, naively (bad results, not discussed in paper)
- deathTime.py
  - Outputs relate to the death time devation experiments
    - data is directly printed to the console
- logicalLocality.py
  - Outputs relate to the logical locality experiments
    - DEFAULT_TRACE_PATH must point to the formatted trace (already set to Workload F, f2fs)
    - data is directly printed to the console
- graphingUtils.py
  - Outputs relate to logical locality graphs
    - graphs are generated using pylot; the graphs will be generated in a separate window
