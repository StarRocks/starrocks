# Elasticsearch catalog

StarRocks supports Elasticsearch catalogs from v3.1 onwards.

StarRocks and Elasticsearch are both popular analytical systems with distinct strengths. StarRocks excels in large-scale distributed computing and supports querying data from Elasticsearch through external tables. Elasticsearch is known for its full-text search capabilities. The combination of StarRocks and Elasticsearch provides a more comprehensive OLAP solution. With Elasticsearch catalogs, you can directly analyze all indexed data in your Elasticsearch cluster by using SQL statements on StarRocks without the need for data migration.

Unlike catalogs for other data sources, an Elasticsearch catalog has only one database named `default` in it upon creation. Each Elasticsearch index is automatically mapped to a data table and mounted to the `default` database.

## Create an Elasticsearch catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### Parameters

#### `catalog_name`

The name of the Elasticsearch catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### `comment`

The description of the Elasticsearch catalog. This parameter is optional.

#### PROPERTIES

The properties of the Elasticsearch catalog. The following table describes the properties supported for Elasticsearch catalogs.

| Parameter                   | Required | Default value | Description                                                  |
| --------------------------- | -------- | ------------- | ------------------------------------------------------------ |
| hosts                       | Yes      | None          | The connection address of the Elasticsearch cluster. You can specify one or more addresses. StarRocks can parse the Elasticsearch version and index shard allocation from this address. StarRocks communicates with your Elasticsearch cluster based on the address returned by the `GET /_nodes/http` API operation. Therefore, the value of the `hosts` parameter must be the same as the address returned by the `GET /_nodes/http` API operation. Otherwise, BEs may not be able to communicate with your Elasticsearch cluster. |
| type                        | Yes      | None          | The type of the data source. Set this parameter to `es` when you create an Elasticsearch catalog. |
| user                        | No       | Empty         | The username that is used to log in to the Elasticsearch cluster with HTTP basic authentication enabled. Make sure that you have permissions to access paths such as `/cluster/state/ nodes/http` and have permissions to read the index. |
| password                    | No       | Empty         | The password that is used to log in to the Elasticsearch cluster. |
| es.type                     | No       | _doc          | The type of the index. If you want to query data in Elasticsearch 8 and later versions, you do not need to configure this parameter because the mapping types have been removed in Elasticsearch 8 and later versions. |
| es.nodes.wan.only           | No       | FALSE         | Specifies whether StarRocks only uses the addresses specified by `hosts` to access the Elasticsearch cluster and fetch data.<ul><li>`true`: StarRocks only uses the addresses specified by `hosts` to access the Elasticsearch cluster and fetch data and does not sniff data nodes on which the shards of the Elasticsearch index reside. If StarRocks cannot access the addresses of the data nodes inside the Elasticsearch cluster, you need to set this parameter to `true`.</li><li>`false`: StarRocks uses the addresses specified by `hosts` to sniff data nodes on which the shards of the Elasticsearch cluster indexes reside. After StarRocks generates a query execution plan, BEs directly access the data nodes inside the Elasticsearch cluster to fetch data from the shards of indexes. If StarRocks can access the addresses of the data nodes inside the Elasticsearch cluster, we recommend that you retain the default value `false`.</li></ul> |
| [es.net](http://es.net).ssl | No       | FALSE         | Specifies whether the HTTPS protocol can be used to access the Elasticsearch cluster. Only StarRocks v2.4 and later support configuring this parameter.<ul><li>`true`: Both the HTTPS and HTTP protocols can be used to access your Elasticsearch cluster.</li><li>`false`: Only the HTTP protocol can be used to access your Elasticsearch cluster.</li></ul> |
| enable_docvalue_scan        | No       | TRUE          | Specifies whether to obtain the values of the target fields from Elasticsearch columnar storage. In most cases, reading data from columnar storage outperforms reading data from row storage. |
| enable_keyword_sniff        | No       | TRUE          | Specifies whether to sniff TEXT-type fields in Elasticsearch based on KEYWORD-type fields. If this parameter is set to `false`, StarRocks performs matching after tokenization. |

### Examples

The following example creates an Elasticsearch catalog named `es_test`:

```SQL
CREATE EXTERNAL CATALOG es_test
COMMENT 'test123'
PROPERTIES
(
    "type" = "es",
    "es.type" = "_doc",
    "hosts" = "https://xxx:9200",
    "es.net.ssl" = "true",
    "user" = "admin",
    "password" = "xxx",
    "es.nodes.wan.only" = "true"
);
```

## Predicate pushdown

StarRocks supports pushing the predicates specified in queries against Elasticsearch tables down to Elasticsearch for execution. This minimizes the distance between the query engine and the storage source and improves query performance. The following table lists the operators that can be pushed down to Elasticsearch.

| SQL syntax   | Elasticsearch syntax  |
| ------------ | --------------------- |
| `=`            | term query            |
| `in`           | terms query           |
| `>=, <=, >, <` | range                 |
| `and`          | bool.filter           |
| `or`           | bool.should           |
| `not`          | bool.must_not         |
| `not in`       | bool.must_not + terms |
| `esquery`      | ES Query DSL          |

## Query examples

The `esquery()` function can be used to push Elasticsearch queries, such as match and geoshape queries, that cannot be expressed in SQL down to Elasticsearch for filtering and processing. In the `esquery()` function, the first parameter that specifies a column name is used to associate with the index, and the second parameter is an Elasticsearch query's Elasticsearch Query DSL-based JSON representation enclosed in curly braces (`{}`). The JSON representation can and must have only one root key, such as `match`, `geo_shape`, or `bool`.

- Match query

  ```SQL
  SELECT * FROM es_table WHERE esquery(k4, '{
     "match": {
        "k4": "StarRocks on elasticsearch"
     }
  }');
  ```

- Geoshape query

  ```SQL
  SELECT * FROM es_table WHERE esquery(k4, '{
  "geo_shape": {
     "location": {
        "shape": {
           "type": "envelope",
           "coordinates": [
              [
                 13,
                 53
              ],
              [
                 14,
                 52
              ]
           ]
        },
        "relation": "within"
     }
  }
  }');
  ```

- Boolean query

  ```SQL
  SELECT * FROM es_table WHERE esquery(k4, ' {
     "bool": {
        "must": [
           {
              "terms": {
                 "k1": [
                    11,
                    12
                 ]
              }
           },
           {
              "terms": {
                 "k2": [
                    100
                 ]
              }
           }
        ]
     }
  }');
  ```

## Usage notes

- From v5.x onwards, Elasticsearch adopts a different data scanning method. StarRocks only supports querying data from Elasticsearch v5.x and later.
- StarRocks only supports querying data from Elasticsearch clusters that have HTTP basic authentication enabled.
- Some queries, such as `count()`-involved queries, run much slower on StarRocks than on Elasticsearch, because Elasticsearch can directly read the metadata related to the specified number of documents that meet the query conditions without the need to filter the requested data.
