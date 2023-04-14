# Get a list of databases
## Request
```http request
GET /api/meta/_databases
```
## Response
```json
{
    "count":4,
    "databases":[
        "_statistics_",
        "information_schema",
        "sink_test",
        "tpch_flink"
    ],
    "status":200
}
```
### Response fields
- status: The status of the API response.
- databases: A list of databases.
- count: The number of databases.
# Get a list of tables
## Request
```http request
GET /api/meta/{database}/_tables
```
### Request parameters
- {database}: Name of the database.

## Response
```json
{
  "database":"sink_test",
  "tables":[
    "sink_test_table",
    "table_hash1"
  ],
  "table count":2,
  "status":200
}
```

###Response fields
- status: The status of the API response.
- database: Database to which these tables belong.
- tables: A list of tables.
- table count: The number of tables in the data.
# Get detailed information about a table
## Request
```http request
GET /api/meta/{database}/{table}/_detail?with_mv=1&with_property=1
```
### Request parameters
- {database}: Name of the database.
- {table}: Name of the table.
- with_mv: Optional parameter. If not specified, the table structure of the base table will be returned by default. If specified, information about all materialized views will also be returned.
- with_property: Optional parameter. If specified, all configuration information will be returned.
## Response
```json
{
  "table":{
    "engineType":"OLAP",
    "schemaInfo":{
      "schemaMap":{
        "mv_customer_sales":{
          "schemaList":[
            {
              "field":"customer_id",
              "type":"INT",
              "isNull":"true",
              "defaultVal":null,
              "key":"true",
              "aggrType":"None",
              "comment":""
            },
            {
              "field":"amount",
              "type":"DECIMAL64(10,2)",
              "isNull":"true",
              "defaultVal":null,
              "key":"false",
              "aggrType":"SUM",
              "comment":""
            }
          ],
          "keyType":"AGG_KEYS",
          "baseIndex":false
        },
        "orders":{
          "schemaList":[
            {
              "field":"order_id",
              "type":"INT",
              "isNull":"true",
              "defaultVal":null,
              "key":"true",
              "aggrType":"None",
              "comment":""
            },
            {
              "field":"customer_id",
              "type":"INT",
              "isNull":"true",
              "defaultVal":null,
              "key":"true",
              "aggrType":"None",
              "comment":""
            },
            {
              "field":"order_date",
              "type":"DATE",
              "isNull":"true",
              "defaultVal":null,
              "key":"true",
              "aggrType":"None",
              "comment":""
            },
            {
              "field":"amount",
              "type":"DECIMAL64(10,2)",
              "isNull":"true",
              "defaultVal":null,
              "key":"false",
              "aggrType":"NONE",
              "comment":""
            }
          ],
          "keyType":"DUP_KEYS",
          "baseIndex":true
        }
      }
    },
    "distributionInfo":{
      "distributionInfoType":"HASH",
      "bucketNum":10,
      "distributionColumns":[
        "order_id"
      ]
    },
    "partitionInfo":{
      "partitionType":"UNPARTITIONED",
      "partitionColumns":null
    },
    "properties":{
      "enable_persistent_index":"false",
      "replicated_storage":"false",
      "write_quorum":"MAJORITY",
      "unique_constraints":"",
      "storage_format":"DEFAULT",
      "replication_num":"3",
      "compression":"LZ4_FRAME",
      "in_memory":"false",
      "foreign_key_constraints":""
    }
  },
  "status":200
}
 ```
### Response fields
- status: The status of the API response.
- table: Detailed information about the data table.
    - engineType: The type of engine being used.
    - schemaInfo: The schema information for the table.
        - schemaMap: Detailed schema information for the table.
    - distributionInfo: Distribution information.
    - partitionInfo: Partition information.
    - properties: Properties of the table.
