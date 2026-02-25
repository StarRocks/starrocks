---
displayed_sidebar: docs
---

# Error Codes

This section describes the common error codes for query requests.

|Serial number|Error code|Error message|
| --- | --- | --- |
|1005| Table creation failed. Returns a specific reason. |
|1007| Cannot create a database with the same name.|
|1008| Cannot delete non-existent database. |
|1044| Cannot access unauthorized database. |
|1045| Username and password do not match and therefore cannot access the system. |
|1046|The target database is not specified. |
|1047|An invalid command is specified. |
|1049| An invalid database is specified. |
|1050| A table with the same name already exists.|
|1051| An invalid table is specified. |
|1052| The specified column name is ambiguous and therefore the corresponding column cannot be uniquely identified. |
|1053|An illegal data column was specified for the Semi-Join/Anti-Join query. |
|1054|The specified column does not exist in the table.|
|1058|The number of columns selected in the query statement does not match the number of columns in the query result. |
|1060|There are duplicate column names. |
|1064|There is no surviving BE node. |
|1066|Duplicate table alias appears in the query statement. |
|1094|Thread ID is invalid. |
|1095|The non-owner of a thread cannot terminate the thread. |
|1096|The query statement does not specify the table to be queried.|
|1102|The database name is incorrect. |
|1104|The table name is incorrect.|
|1105|Other errors. |
|1110|Duplicate columns were specified in the subquery.|
|1111|Illegal use of aggregation function in `WHERE` clause|
|1113| The set of columns in the new table cannot be empty. |
|1115|Unsupported character sets are used.|
|1130|An unauthorized IP address is used by the client. |
|1132|No permission to change user password.|
|1141| Specified entries don’t have privileges to revoke. |
|1142|An unauthorized action was performed. |
|1166| The data column name is incorrect. |
|1193| System variable has invalid name |
|1203| The number of active connections used exceeded the limit. |
|1211| Not allowed to create new users. |
|1227| The user has performed an out-of-authority operation. |
|1228|Session variables cannot be modified by the `SET GLOBAL` command. |
|1229|Global variables should be modified by the `SET GLOBAL` command. |
|1230|Related system variables do not have default values. |
|1231|An invalid value was set for a system variable.|
|1232|A value of the wrong data type was set for a system variable. |
|1248|No alias was set for an inline view.|
|1251|The client does not support the user authentication protocol required by the server. |
|1286|The storage engine is incorrectly configured. |
|1298|The time zone is incorrectly configured.|
|1347|The object does not match the expected type. |
|1353|The specified column number in the `Select` clause of the view is not equal to the defined column number. |
|1364|No default value is set for columns that do not allow NULL values. |
|1372|The password is not long enough. |
|1396|The performed operation failed.|
|1471|The specified table is not allowed to insert data. |
|1507|Delete nonexistent partition, and no condition is specified to only delete existing partitions. |
|1508|All partitions should be deleted by a delete table operation. |
|1517|There are duplicate partition names.|
|1524|The specified plugin has not been loaded.|
|1567|The name of the partition is incorrect. |
|1621|The specified system variable is read-only. |
|1735|The specified partition name does not exist in the table.|
|1748|Data cannot be inserted into a table that does not have a partition.|
|1749|The specified partition does not exist. |
|5000|The specified table is not an OLAP table. |
|5001|The specified storage path is invalid. |
|5002|The name of the specified column should be displayed.|
|5003|The dimension column should be preceded by the index column. |
|5004|The table should contain at least 1 dimension column. |
|5005|The cluster ID is invalid. |
|5006|Invalid query plan. |
|5007|Conflicting query plans. |
|5008|The data insert is only available for data tables with partitions. |
|5009|The `PARTITION` clause cannot be used to insert data into tables without partitions. |
|5010|The number of columns in the table to be created is not equal to the number of columns in the `SELECT` clause. |
|5011|Table reference could not be accessed. |
|5012|The specified value is not a valid number. |
|5013|The time unit is not supported. |
|5014|The table status is not normal. |
|5015|The partition status is not normal. |
|5016| A data import job exists in the partition. |
|5017| The specified column is not a dimension column. |
|5018| The format of the value is invalid. |
|5019| The data replica does not match the version. |
|5021| The BE node is offline. |
|5022| The number of partitions in a non-partitioned table is not 1|
|5023| No action was specified in the statement used to modify the table or data. |
|5024| Job execution timed out. |
|5025| Data insertion failed. |
|5026| An unsupported data type was used when creating a table via the `SELECT` statement. |
|5027| The specified parameter was not set. |
|5028| The specified cluster was not found. |
|5030| The user does not have permission to access the cluster. |
|5031| No parameter specified or invalid parameter. |
|5032| The number of cluster instances was not specified. |
|5034| A cluster with the same name already exists.|
|5035| The number of cluster instances is incorrectly configured. |
|5036|Insufficient BE nodes in the cluster. |
|5037|All databases should be deleted before deleting the cluster.|
|5038| The BE node with the specified ID does not exist in the cluster.|
|5040|No cluster with the same name exists.|
|5042|No permissions. |
|5043|The number of instances should be greater than 0.|
|5046|Source cluster does not exist.|
|5047| Destination cluster does not exist. |
|5048| Source database does not exist. |
|5049| Destination database does not exist.|
|5050| No cluster selected. |
|5051| The source database should be associated with the destination database first. |
|5052| Intra-cluster error: BE node information is incorrect. |
|5053| There is no migration task from the source database to the destination database. |
|5054| The specified database has been associated with the destination database, or data is being migrated. |
|5055| Database associations or data migrations cannot be performed within the same cluster. |
|5056| Database cannot be deleted: it is associated with another database or data is being migrated.|
|5056| Database cannot be renamed: it is associated with another database or data is being migrated. |
|5056| Insufficient BE nodes in the cluster. |
|5056| The specified number of BE nodes already exists in the cluster. |
|5059| There are BE nodes in the cluster that are in the offline state. |
|5063| The type name is incorrect. |
|5064| Generic error message.|
|5063|The Colocate feature has been disabled by the administrator. |
|5063|A colocate data table with the same name does not exist. |
|5063|The Colocate table must be an OLAP table.|
|5063|Colocate tables should have the same number of replicas. |
|5063|Colocate tables should have the same number of split buckets. |
|5063|Colocate tables should have the same number of partition columns. |
|5063|Colocate tables should have the same data type of partitioned columns. |
|5064|The specified table is not a colocate table.|
|5065|The specified operation is invalid.|
|5065|The specified time unit is illegal. The correct units are `DAY`, `WEEK`, and `MONTH`. |
|5066|The start value of the dynamic partition should be less than 0.|
|5066|The start value of the dynamic partition is not a valid number. |
|5066|The end value of the dynamic partition should be greater than 0. |
|5066|The end value of the dynamic partition is not a valid number. |
|5066|The end value of the dynamic partition is null. |
|5067|The bucket number of the dynamic partition should be greater than 0. |
|5067|The bucket number of the dynamic partition is not a valid number. |
|5066| The bucket number of the dynamic partition is empty. |
|5068| Whether to allow dynamic partition where the value is not a valid boolean: true or false. |
|5069| The name of the specified dynamic partition has an illegal prefix. |
|5070| The specified operation is disabled. |
|5071| The number of replicas of the dynamic partition should be greater than 0.|
|5072| The number of replicas of the dynamic partition is not a valid number. |
