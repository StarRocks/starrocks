# Error Codes

This section describes the common error codes for query requests.

|Serial number|Error code|Error message|
| --- | --- | --- |
|1|1005| Table creation failed. Returns a specific reason. |
|2|1007| Cannot create a database with the same name.|
|3|1008| Cannot delete non-existent database. |
|4|1044| Cannot access unauthorized database. |
|5|1045| Username and password do not match and therefore cannot access the system. |
|6|1046|The target database is not specified. |
|7|1047|An invalid command is specified. |
|8|1049| An invalid database is specified. |
|9|1050| A table with the same name already exists.|
|10|1051| An invalid table is specified. |
|11|1052| The specified column name is ambiguous and therefore the corresponding column cannot be uniquely identified. |
|12|1053|An illegal data column was specified for the Semi-Join/Anti-Join query. |
|13|1054|The specified column does not exist in the table.|
|14|1058|The number of columns selected in the query statement does not match the number of columns in the query result. |
|15|1060|There are duplicate column names. |
|16|1064|There is no surviving BE node. |
|17|1066|Duplicate table alias appears in the query statement. |
|18|1094|Thread ID is invalid. |
|19|1095|The non-owner of a thread cannot terminate the thread. |
|20|1096|The query statement does not specify the table to be queried.|
|21|1102|The database name is incorrect. |
|22|1104|The table name is incorrect.|
|23|1105|Other errors. |
|24|1110|Duplicate columns were specified in the subquery.|
|25|1111|Illegal use of aggregation function in `WHERE` clause|
|26|1113| The set of columns in the new table cannot be empty. |
|27|1115|Unsupported character sets are used.|
|28|1130|An unauthorized IP address is used by the client. |
|29|1132|No permission to change user password.|
|30|1141| Specified entries don’t have privileges to revoke. |
|31|1142|An unauthorized action was performed. |
|32|1166| The data column name is incorrect. |
|33|1193| System variable has invalid name |
|34|1203| The number of active connections used exceeded the limit. |
|35|1211| Not allowed to create new users. |
|36|1227| The user has performed an out-of-authority operation. |
|37|1228|Session variables cannot be modified by the `SET GLOBAL` command. |
|38|1229|Global variables should be modified by the `SET GLOBAL` command. |
|39|1230|Related system variables do not have default values. |
|40|1231|An invalid value was set for a system variable.|
|41|1232|A value of the wrong data type was set for a system variable. |
|42|1248|No alias was set for an inline view.|
|43|1251|The client does not support the user authentication protocol required by the server. |
|44|1286|The storage engine is incorrectly configured. |
|45|1298|The time zone is incorrectly configured.|
|46|1347|The object does not match the expected type. |
|47|1353|The specified column number in the `Select` clause of the view is not equal to the defined column number. |
|48|1364|No default value is set for columns that do not allow NULL values. |
|49|1372|The password is not long enough. |
|50|1396|The performed operation failed.|
|51|1471|The specified table is not allowed to insert data. |
|52|1507|Delete nonexistent partition, and no condition is specified to only delete existing partitions. |
|53|1508|All partitions should be deleted by a delete table operation. |
|54|1517|There are duplicate partition names.|
|55|1524|The specified plugin has not been loaded.|
|56|1567|The name of the partition is incorrect. |
|57|1621|The specified system variable is read-only. |
|58|1735|The specified partition name does not exist in the table.|
|59|1748|Data cannot be inserted into a table that does not have a partition.|
|60|1749|The specified partition does not exist. |
|61|5000|The specified table is not an OLAP table. |
|62|5001|The specified storage path is invalid. |
|63|5002|The name of the specified column should be displayed.|
|64|5003|The dimension column should be preceded by the index column. |
|65|5004|The table should contain at least 1 dimension column. |
|66|5005|The cluster ID is invalid. |
|67|5006|Invalid query plan. |
|68|5007|Conflicting query plans. |
|69|5008|The data insert is only available for data tables with partitions. |
|70|5009|The `PARTITION` clause cannot be used to insert data into tables without partitions. |
|71|5010|The number of columns in the table to be created is not equal to the number of columns in the `SELECT` clause. |
|72|5011|Table reference could not be accessed. |
|73|5012|The specified value is not a valid number. |
|74|5013|The time unit is not supported. |
|75|5014|The table status is not normal. |
|76|5015|The partition status is not normal. |
|77|5016| A data import job exists in the partition. |
|78|5017| The specified column is not a dimension column. |
|79|5018| The format of the value is invalid. |
|80|5019| The data replica does not match the version. |
|81|5021| The BE node is offline. |
|82|5022| The number of partitions in a non-partitioned table is not 1|
|83|5023| No action was specified in the statement used to modify the table or data. |
|84|5024| Job execution timed out. |
|85|5025| Data insertion failed. |
|86|5026| An unsupported data type was used when creating a table via the `SELECT` statement. |
|87|5027| The specified parameter was not set. |
|88|5028| The specified cluster was not found. |
|89|5030| The user does not have permission to access the cluster. |
|90|5031| No parameter specified or invalid parameter. |
|91|5032| The number of cluster instances was not specified. |
|92|5034| A cluster with the same name already exists.|
|93|5035| The number of cluster instances is incorrectly configured. |
|94|5036|Insufficient BE nodes in the cluster. |
|95|5037|All databases should be deleted before deleting the cluster.|
|97|5038| The BE node with the specified ID does not exist in the cluster.|
|98|5040|No cluster with the same name exists.|
|99|5041|No cluster name specified.|
|100|5042|No permissions. |
|101|5043|The number of instances should be greater than 0.|
|102|5046|Source cluster does not exist.|
|103|5047| Destination cluster does not exist. |
|104|5048| Source database does not exist. |
|105|5049| Destination database does not exist.|
|106|5050| No cluster selected. |
|107|5051| The source database should be associated with the destination database first. |
|108|5052| Intra-cluster error: BE node information is incorrect. |
|109|5053| There is no migration task from the source database to the destination database. |
|110|5054| The specified database has been associated with the destination database, or data is being migrated. |
|111|5055| Database associations or data migrations cannot be performed within the same cluster. |
|112|5056| Database cannot be deleted: it is associated with another database or data is being migrated.|
|113|5056| Database cannot be renamed: it is associated with another database or data is being migrated. |
|114|5056| Insufficient BE nodes in the cluster. |
|115|5056| The specified number of BE nodes already exists in the cluster. |
|116|5059| There are BE nodes in the cluster that are in the offline state. |
|117|5062| The cluster name is illegal: `default_cluster` is the reserved name.|
|118|5063| The type name is incorrect. |
|119|5064| Generic error message.|
|120|5063|The Colocate feature has been disabled by the administrator. |
|121|5063|A colocate data table with the same name does not exist. |
|122|5063|The Colocate table must be an OLAP table.|
|123|5063|Colocate tables should have the same number of replicas. |
|124|5063|Colocate tables should have the same number of split buckets. |
|125|5063|Colocate tables should have the same number of partition columns. |
|126|5063|Colocate tables should have the same data type of partitioned columns. |
|127|5064|The specified table is not a colocate table.|
|128|5065|The specified operation is invalid.|
|129|5065|The specified time unit is illegal. The correct units are `DAY`, `WEEK`, and `MONTH`. |
|130|5066|The start value of the dynamic partition should be less than 0.|
|131|5066|The start value of the dynamic partition is not a valid number. |
|132|5066|The end value of the dynamic partition should be greater than 0. |
|133|5066|The end value of the dynamic partition is not a valid number. |
|134|5066|The end value of the dynamic partition is null. |
|135|5067|The bucket number of the dynamic partition should be greater than 0. |
|136|5067|The bucket number of the dynamic partition is not a valid number. |
|137|5066| The bucket number of the dynamic partition is empty. |
|138|5068| Whether to allow dynamic partition where the value is not a valid boolean: true or false. |
|139|5069| The name of the specified dynamic partition has an illegal prefix. |
|140|5070| The specified operation is disabled. |
|141|5071| The number of replicas of the dynamic partition should be greater than 0.|
|142|5072| The number of replicas of the dynamic partition is not a valid number. |
