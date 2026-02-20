# SQLDSS (HEC-DSS v8) #
## Performance Comparisons ##

In the comparisons that have been available to be tested so far, SQLDSS compares favorably to HEC-DSS v7 in performance.

The numbers in this page come from a run of the test <code>mil.army.usace.hec.sqldss.Dss7ComparisonTest.compareToDss7</code>

### Opening Files ##

| Action             | HEC-DSS v7 (ms) | SQLDSS (ms) | Time Factor |
|--------------------|-----------------|-------------|-------------|
| Open Existing File | 8.57            | 0.43        | 0.05        |
| Create New File    | 8.29            | 14.14       | 1.71        |

### Reading/Writing Regular Time Series ###

| Action                  | HEC-DSS v7 (ms) | SQLDSS (ms) | Time Factor |
|-------------------------|-----------------|-------------|-------------|
| Read 10 Records         | 16              | 16          | 1.00        |
| Read 100 Records        | 88              | 110         | 1.25        |
| Read 1000 Records       | 616             | 920         | 1.50        |
| Read 1000 Records       | 616             | 920         | 1.50        |
| Read 10000 Records      | 5114            | 8137        | 1.59        |
| Read 41759 Records      | 20300           | 33942       | 1.67        |
|                         |                 |             |             |      
| Write 10 New Records    | 19              | 16          | 0.84        |
| Write 100 New Records   | 54              | 63          | 1.17        |
| Write 1000 New Records  | 543             | 236         | 0.44        |
| Write 10000 New Records | 5283            | 1902        | 0.36        |
| Write 41759 New Records | 21700           | 8280        | 0.38        |
|                         |                 |             |             |      
| Overwrite 10 Records    | 13              | 18          | 1.39        |
| Overwrite 100 Records   | 56              | 31          | 0.55        |
| Overwrite 1000 Records  | 503             | 210         | 0.42        |
| Overwrite 10000 Records | 4788            | 1882        | 0.39        |
| Overwrite 41759 Records | 19432           | 7919        | 0.41        |

### File Size ###

| Time Series Records | HEC-DSS v7 (bytes) | SQLDSS (bytes) | Size Factor |
|---------------------|--------------------|----------------|-------------|
| 0                   | 126192             | 303104         | 2.40        |
| 1                   | 127720             | 303104         | 2.37        |
| 10                  | 144792             | 344064         | 2.38        |
| 100                 | 465816             | 712704         | 1.53        |
| 1000                | 3317600            | 4554752        | 1.37        |
| 10000               | 27822416           | 43032576       | 1.55        |
| 41759               | 177766400          | 177766400      | 1.96        |
