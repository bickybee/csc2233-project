# Running Experiments

We created a variety of scripts to help run our experiments.
After setting up the pipenv environment, navigate to the source folder and run scripts using

```
pipenv run python script_name.py
```

For the following scripts, the trace source folder can be passed as a command-line argument, but defaults to our data/formatted folder. See the files in that folder for trace formatting reference-- we added column headers and removed excess blkparse data from the ends of the outputs.

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
- deathTimes.py
  - Outputs relate to the death time devation experiments
    - experiments resulted in less data, so was directly output to the console
    
