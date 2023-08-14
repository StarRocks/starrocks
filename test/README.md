# SQL-tester
This is an introduction for the SQL-tester project.  

## About
SQL-tester is a SQL testing framework for StarRocks , designed to reduce the cost of test case construction. It uses SQL language to write test cases and has the ability to verify test results.

### [**QuickStart**](#QuickStart)
Documentation on getting started, installation and simple use.  

### [**Run Parameters**](#Parameters)
Detailed description of the execution parameters.  

### [**Grammar**](#Grammar)  
Reference documentation for the syntax format and other functions.


# <span id="QuickStart">QuickStart</span>
## Prerequisites
### 1. Python

<table>
  <tr>
    <td>Python</td>
    <td>>= 3.6.8</td>
  </tr>
</table>

### 2. Pip
```shell
yum -y install python-pip
python3 -m pip install --upgrade pip
```

### 3. Requirements
Information about the dependencies is stored in the `requirements.txt`.   
You can install them by:  
```shell
$ python3 -m pip install -r requirements.txt
```

### 4. Config
First you need to prepare a target sr to be tested, and then store their information in `conf/sr.conf`.  
* The mysql-client section stores the basic information about the database connection.
* The replace section holds the variable arguments, you can freely add and use them in shell syntax as `${param}` (we will talk about them later).

- DEMO:  
```yaml
[mysql-client]
host = 172.26.195.96
port = 9032
user = root
password = 
http_port = 8034

[replace]
url = http://${mysql-client:host}:${mysql-client:http_port}
```
### 5. Test Data
To use the data `${DATA}` like ssb/tpcds/tpch, you need to download them to the dir: `common/data/[ssb|tpch|tpcds]`. Then you can load them in case like: `function: prepare_data("${DATA}", "${db[0]}")`

## Running your test
```shell
# run concurrent cases
$ python3 run.py 
# run sequential cases
$ python3 run.py -a sequential -c 1 -v
```

# <span id="Parameters">Run Parameters<span>

This describes the parameters for running the test cases.


## USAGE

```shell
$ python3 run.py [-d|--dir=] [-v] [-r] [-p] [-l] [-c|--concurrency=] [-t|--timeout=] [-a|--attr=] [--file_filter=] [--case_filter=]
```

## Introduction

**`-d|--dir=` [Optional]**  
We use it to specify the path where the target use case to be run is stored. The default value is `./sql`. You can run all the use cases without specifying it. The value supports both directory and use case paths.

**`-r|--record` | `-v|--validate` [Optional. Default: -v]**  
There are two types of test files in SQL-tester:  
- T  
File stores the SQL statements of the cases.
- R  
File stores the SQL statements and the execution results that need to be verified of the use cases.  

Accordingly, there are two modes of operation.  
- RECORD MODE`[-r|--record]`  
Cases in the files of type T will be executed and the results will be saved to the corresponding R file in order to use them in VALIDATE MODE in the future.
- VALIDATE MODE`[-v|--validate]  
Cases in the files of type R will be executed, the framework will compare the actual results with the expected results in the file. If they do not match, the case is fail.

**`-p|--part` [Optional]**  
It works only in record mode. If you add this parameter, the step of saving the test results, only the information of the executed cases will be updated, and the other cases in the file will not be affected. Otherwise, all the file contents will be overwritten.

**`-c|--concurrency=` [Optional. Default: 8]**  
Number of concurrent use cases to run, default is 8.

**`-t|--timeout=` [Optional. Default: 600]**  
The execution timeout(s) for a single case, default is 10 min. Once the case's running time exceeds the threshold, it will fail and exit.

**`-l|--list` [Optional]**  
With this parameter, the framework will just list the names of the cases that need to be executed, but will not actually execute them.

**`-a|--attr` [Optional]**  
tag filters, format: tag1,tag2...

**`--file_filter=` [Optional]**  
The format of the values is a regular expression, and only test cases in files with filenames that match it will be executed.   
If you're not sure of the scope of impact, you can list them first with `-l` firstly.

**`--case_filter=` [Optional]**  
The format of the value is a regular expression, and only test cases whose names match it will be executed.

**`--skip_reruns` [Optional]**
By default, all cases will run 3 times and passed 3 times, if this parameter provided, all case will be run exactly only once, default False.

# <span id="Grammar">Grammar</span>
This describes the grammar of SQL-tester framework, and how to add your cases.

## Add Cases

### 1. Path
Default: `./sql`  

### 2. Content of case files
You can store multiple test cases in a single case file. They will be saved in the T or R directories by default.
### 2.1. T
It is used to store all the execution statements of the test case, and you can list them in an orderly manner.  If you don't want to write the R file manually, you can run the use case through `python3 run.py -d ${T} -r` and generate the corresponding R file
The first line contains the name of the test case in the format: `-- name: ${case name}`. Name format: `[a-zA-Z0-9_-]+`  
Starting from the second line, it lists all the test statements to be executed, such as `create, select, drop`...  

- Format:
```sql
-- name: ${case name}
SQL1;
SQL2;
SQL3;
```

- Example:
```sql
-- name: test_alter_array_partition
show backends;
select connection_id();
create database test_array_ee2a58b4_64a9_11ed_9ab7_00163e0b9de0;
use test_array_ee2a58b4_64a9_11ed_9ab7_00163e0b9de0;
create table t0(c0 INT, c1 array<int>)duplicate key(c0) partition by range(`c0`)(PARTITION `p100` VALUES LESS THAN ('100'),PARTITION `p1000` VALUES LESS THAN ('1000'),PARTITION `p10000` VALUES LESS THAN ('10000'))distributed by hash(c0) buckets 1 properties('replication_num'='1');
insert into t0 values(99, [1,2,3]),(999, [4,5,6]),(9999, [7,8,9]);
alter table t0 RENAME PARTITION p100 p99;
select * from t0 order by c0;
drop database test_array_ee2a58b4_64a9_11ed_9ab7_00163e0b9de0;
```

### 2.2. R
In addition to the test statements, the R file will also hold information about the results to be verified.  
The first line is the same with T.  
Starting from the second line, in addition to listing the SQL to be executed, if you also want to check the result of his execution, you need to follow the result information immediately after the test statement and wrap them with `-- result: ` and `-- !result`.
- Format:
```sql
-- name: ${case name}
SQL1;
-- result: 
RES LINE1
RES LINE2
-- !result
SQL2;
```

- Example:
```sql
-- name: test_alter_array_partition
show backends;
select connection_id();
create database test_array_ee2a58b4_64a9_11ed_9ab7_00163e0b9de0;
use test_array_ee2a58b4_64a9_11ed_9ab7_00163e0b9de0;
create table t0(c0 INT, c1 array<int>)duplicate key(c0) partition by range(`c0`)(PARTITION `p100` VALUES LESS THAN ('100'),PARTITION `p1000` VALUES LESS THAN ('1000'),PARTITION `p10000` VALUES LESS THAN ('10000'))distributed by hash(c0) buckets 1 properties('replication_num'='1');
insert into t0 values(99, [1,2,3]),(999, [4,5,6]),(9999, [7,8,9]);
alter table t0 RENAME PARTITION p100 p99;
select * from t0 order by c0;
-- result: 
99	[1,2,3]
999	[4,5,6]
9999	[7,8,9]
-- !result
drop database test_array_ee2a58b4_64a9_11ed_9ab7_00163e0b9de0;
```

## Function
### 1. Skip Check
It shows ways to skip result checking in record mode.
#### 1.1 SKIP ARRAY
`skip_res_cmd` array in `lib/skip.py` holds all the statement expressions that need to be filtered and checked.  
For all use cases, if the execution finds that the SQL satisfies any of the regular expressions, the framework will skip the check round.
- Example:
```python
skip_res_cmd = [
    "${SKIP_SQL_REG_1}",
    "${SKIP_SQL_REG_2}"
]
```

#### 1.2 SKIP FLAG
You can skip the check round by adding the **`[UC]`** flag in front of the statement that needs to skip the check.  
Compared to `SKIP ARRAY`, `[UC]` is more flexible as it affects a single case without affecting other cases.
```sql
-- name: ${case name}
[UC]SQL1;
-- result: 
RES LINE1
RES LINE2
-- !result
SQL2;
```

### 2. SHELL
The test framework supports shell commands, you just need to add **`shell: `** in front of the command. Also, the framework supports the use of variables in commands to some extent. Some variables are naturally supported, such as root_path and so on. Of course, you can set custom variables in the `replace` section of `conf/sr.conf` and use it in the format of "${variable name}".  
The shell statement will not check the return result by default, and the use case will fail as soon as it is executed with an error.When the result is set, both return code / stdout(stderr) will be checked.  
- format:
```sql
shell: ***
-- result: 
returncode
stdout/stderror
-- !result
```

- Example (ADD VARIABLE):
```yaml
# conf/sr.conf
[replace]
VAR_1 = ABCDEFG
```

- Example (USE VARIABLE):
```sql
-- name: ${case name}
SQL1;
shell: shell_command
SQL2;
```

- Example (NORMAL):
```sql
-- name: ${case name}
SQL1;
shell: echo ${VAR_1}
SQL2;
```

- Example (JSON CHECK):
```sql
-- name: ${case name}
SQL1;
shell: curl *****
-- result: 
0
{
    "check_field_1": "value1",
    "check_field_2": "value2"
}
-- !result
SQL2;
```

### 3. FUNCTION
The test framework supports python functions. You can define your functions in the `StarrocksSQLApiLib` class in `lib/sr_sql_lib.py` such as `wait_load_finish`. The use case will continue to run only after the function execution is finished.  
Of course, you can add assertions to the function to guarantee correctness.   
For example, you can wait for the task to finish in the function.

- Example(DEFINE FUNCTION):
```python
def func_1(self, param1, param2):
    # assert statements
    pass
```

- Example(USE FUNCTION):
```sql
- name: ${case name}
SQL1;
function: func_1("var1", "var2")
SQL2;
```

### 4. CHECK IN ORDERLY MANNER
The multi-row check of SQL execution result is unordered by default, but for some statements like order by, we need to guarantee the correct order of result.  
You can check them by prefixing the SQL statements with `[ORDER]` tags.
```sql
-- name: ${case name}
[ORDER]SQL1;
-- result: 
RES LINE1
RES LINE2
-- !result
SQL2;
```


### 5. REGULAR CHECKS FOR SQL RESULTS
Some SQL execution results have variables (such as ID).In addition to using SKIP CHECK and UNCHECK, the framework supports regular checks.  
Note: Before using, please make sure the SQL is not in the SKIP CHECK list.  
Usage: Add the [REGEX] marker before the result to be verified.
```sql
-- name: ${case name}
SQL1;
-- result: 
[REGEX]***
-- !result
```
- Example:
```sql
-- name: test_sql_regex
create table t0 ( c0 int ) distributed by hash(c0) buckets 2 properties("replication_num" = "3");
insert into t0 values (1), (1), (2), (3);
select * from t0 order by c0;
-- result:
[REGEX]1\n1\n2\n3
-- !result
explain select 1;
-- result:
[REGEX].*PARTITION: UNPARTITIONED.*
-- !result
```
