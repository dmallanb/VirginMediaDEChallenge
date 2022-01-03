# Data Engineer Tech Test

## 1. Challenge Instructions
This is a simple tech test asking you to write some Python program with a purpose to verify your learning capability and Python skills.
Please note that we do expect you to have sufficient Python skills but not on the specific tech stack required. The expectation
is that if you don't know about something, learn how to use it by reading and trying to solve the problem. There are
plenty of tutorials and examples online, and you can Google as much as you like to complete the task.

Do get prepared on explaining what you have done, especially when third party code or tutorial code have been used.

#### Overall requirement
1. Once the solution is finished, please store it in a public Git repository on GitHub (this is free to create) and share the link with us
1. The solution should be working e2e, and ideally we would expect to clone the repo and run a single command to get the output.
1. You do not have to use Cloud Dataflow, Direct Runner is fine

##### Task 1
Write an Apache Beam batch job in Python satisfying the following requirements
1. Read the input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
2. Find all transactions have a `transaction_amount` greater than `20`
3. Exclude all transactions made before the year `2010`
4. Sum the total by `date`
5. Save the output into `output/results.json.gz` and make sure all files in the `output/` directory is git ignored

If the output is in a CSV file, it would have the following format
```
date, total_amount
2011-01-01, 12345.00
...
```

##### Task 2
Following up on the same Apache Beam batch job, also do the following
1. Group all transform steps into a single `Composite Transform`
1. Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam


## 2. Notes on Solution
### How to Run
#### Machine Considerations
I ran this successfully on a MacOS machine with Python 3.7 - this set of instructions assumes you have access to either a Linux or MacOS machine with Python 3.7 or 3.8 installed.

Unfortunately using on Windows requires complex troubleshooting with the Anaconda Python distribution (due to how Anaconda handles the SSL library PATH), and the shell script is not compatible with PowerShell.

#### Startup Script
Run the following CLI command when this respository is set as your current directory to run the pipeline:
```shell
sh runme.sh
```

This script does the following:
1. Installs PIP dependencies
2. Runs unit tests in verbose mode
3. Runs `main.py`, which runs Task 1 and Task 2 as separate pipelines sequentially

### Requirements Checklist
| Requirement | Status | Notes | 
| --- | --- | --- |
| **T1** - Pipeline pulls from Google Cloud Storage | &#9745; | Using `beam.io.ReadFromText`, skipping headers. |
| **T1** - Filter for transactions greater than 20 | &#9745; | Implemented with `beam.Filter` with a lambda statement. |
| **T1** - Filter out transactions before 2010 | &#9745; | Implemented with `beam.Filter` with a custom unit tested function. |
| **T1** - Sum total amount - group by date in YYYY-MM-DD format | &#9745; | Implemented using `CombineFn` and `GroupByKey` |
| **T1** - Export files as `output/*.json.gz` with 'date' and 'total_amount' as attributes | &#9745; | Individual records are stored as objects in a JSON array directly, no root JSON element. One file is output per pipeline per run. Outputs from the task 1 pipeline are named `task1-*.json.gz`. |
| **T1** - Apply unit tests to transform and filter functions | &#9745; | Using `unittests` library. |
| **T2** - Group Transforms and filters into a composite transform | &#9745; | Implemented as a second pipeline in `main.py` that runs after the task 1 pipeline. Implemented using `beam.PTransform` class inheritance. |
| **T2** - Apply unit test to composite transform | &#9745; | Using apache beam testing utils in addition to the `unittests` library. |
