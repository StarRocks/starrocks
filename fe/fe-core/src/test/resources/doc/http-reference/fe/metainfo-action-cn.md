# 获取数据库列表
## 请求
```http request
GET /api/meta/_databases
```
## 响应
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
### 响应字段
- status：接口返回状态
- databases：数据库列表
- count：数据库数量

# 获取表列表
## 请求
```http request
GET /api/meta/{database}/_tables
```
### 请求参数
- {database}：数据库名称
## 响应
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
### 响应字段
- status：接口返回状态
- database：所属数据库
- tables：表列表
- table count：数据表数量
  
# 获取表的详细信息
## 请求
```http request
GET /api/meta/{database}/{table}/_detail?with_mv=1&with_property=1
```
### 请求参数
- {database}：数据库名称
- {table}：数据表名称
- with_mv：可选项，如果未指定，默认返回 base 表的表结构。如果指定，则还会返回所有物化视图的信息。
- with_property：可选项，如果指定，会返回所有配置信息。
## 响应
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
### 响应字段
- status：接口返回状态
- table：数据表的具体信息
    - engineType：引擎类型
    - schemaInfo：数据表的schema信息
        - schemaMap：表的具体schema信息
    - distributionInfo：分布信息
    - partitionInfo：分区信息
    - partitionInfo：数据表属性