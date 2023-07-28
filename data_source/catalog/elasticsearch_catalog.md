# Elasticsearch catalog

StarRocks 自 3.1 版本起支持 Elasticsearch Catalog。

StarRocks 与 Elasticsearch 都是目前流行的分析系统。StarRocks 擅长大规模分布式计算，且支持通过外部表查询 Elasticsearch。Elasticsearch 擅长全文检索。两者结合提供了一个更完善的 OLAP 解决方案。基于 Elasticsearch Catalog，您可以直接通过 StarRocks 使用 SQL 分析 Elasticsearch 集群内所有的索引数据，并且无需数据迁移。

区别于其他数据源的 Catalog，Elasticsearch Catalog 创建后，下面只有一个名为 `default` 的数据库 (Database)，每一个 Elasticsearch 索引 (Index) 自动映射一张数据表 (Table)，并且都会自动挂载在 `default` 数据库下面。

## 创建 Elasticsearch Catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### 参数说明

#### `catalog_name`

Elasticsearch Catalog 的名称。命名规则如下：

- 必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。
- 总长度不能超过 1023 个字符。
- Catalog 名称大小写敏感。

#### `comment`

Elasticsearch Catalog 的描述。此参数为可选。

#### PROPERTIES

Elasticsearch Catalog 的属性。支持如下属性：

| **参数**             | **是否必须** | **默认值** | **说明**                                                     |
| -------------------- | ------------ | ---------- | ------------------------------------------------------------ |
| hosts                | 是           | 无         | Elasticsearch 集群连接地址，用于获取 Elasticsearch 版本号以及索引的分片分布信息，可指定一个或多个。StarRocks 是根据 `GET /_nodes/http` API 返回的地址和 Elasticsearch 集群进行通讯，所以 `hosts` 参数值必须和 `GET /_nodes/http` 返回的地址一致，否则可能导致 BE 无法和 Elasticsearch 集群进行正常的通讯。 |
| type                 | 是           | 无         | 数据源的类型。创建 Elasticsearch Catalog 时，必须设置为 `es`。 |
| user                 | 否           | 空         | 开启 HTTP Basic 认证的 Elasticsearch 集群的用户名，需要确保该用户有访问 `/cluster/state/ nodes/http` 等路径权限和对索引的读取权限。 |
| password             | 否           | 空         | 对应用户的密码信息。                                         |
| es.type              | 否           | `_doc`     | 指定索引的类型。如果您要查询的是数据是在 Elasticsearch 8 及以上版本，那么在 StarRocks 中创建外部表时就不需要配置该参数，因为 Elasticsearch 8 以及上版本已经移除了 mapping types。 |
| es.nodes.wan.only    | 否           | `false`    | 表示 StarRocks 是否仅使用 `hosts` 指定的地址，去访问 Elasticsearch 集群并获取数据。自 2.3.0 版本起，StarRocks 支持配置该参数。<ul><li>`true`：StarRocks 仅使用 `hosts` 指定的地址去访问 Elasticsearch 集群并获取数据，不会探测 Elasticsearch 集群的索引每个分片所在的数据节点地址。如果 StarRocks 无法访问 Elasticsearch 集群内部数据节点的地址，则需要配置为 `true`。</li><li>`false`：StarRocks 通过 `hosts` 中的地址，探测 Elasticsearch 集群索引各个分片所在数据节点的地址。StarRocks 经过查询规划后，相关 BE 节点会直接去请求 Elasticsearch 集群内部的数据节点，获取索引的分片数据。如果 StarRocks 可以访问 Elasticsearch 集群内部数据节点的地址，则建议保持默认值 `false`。</li></ul> |
| es.net.ssl           | 否           | `false`    | 是否允许使用 HTTPS 协议访问 Elasticsearch 集群。自 2.4 版本起，StarRocks 支持配置该参数。<ul><li>`true`：允许，HTTP 协议和 HTTPS 协议均可访问。</li><li>`false`：不允许，只能使用 HTTP 协议访问。</li></ul> |
| enable_docvalue_scan | 否           | `true`     | 是否从 Elasticsearch 列式存储获取查询字段的值。多数情况下，从列式存储中读取数据的性能要优于从行式存储中读取数据的性能。 |
| enable_keyword_sniff | 否           | `true`     | 是否对 Elasticsearch 中 TEXT 类型的字段进行探测，通过 KEYWORD 类型字段进行查询。设置为 `false` 会按照分词后的内容匹配。 |

### 创建示例

执行如下语句创建一个名为 `es_test` 的 Elasticsearch Catalog：

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

## 谓词下推

StarRocks 支持对 Elasticsearch 表进行谓词下推，把过滤条件推给 Elasticsearch 进行执行，让执行尽量靠近存储，提高查询性能。目前支持下推的算子见下表：

| SQL syntax   | Elasticsearch syntax  |
| ------------ | --------------------- |
| =            | term query            |
| in           | terms query           |
| >=, <=, >, < | range                 |
| and          | bool.filter           |
| or           | bool.should           |
| not          | bool.must_not         |
| not in       | bool.must_not + terms |
| esquery      | ES Query DSL          |

## 查询示例

通过 `esquery()` 函数将一些无法用 SQL 表述的 Elasticsearch 查询（如 Match 查询和 Geoshape 查询等）下推给 Elasticsearch 进行过滤处理。`esquery()` 的第一个列名参数用于关联索引，第二个参数是 Elasticsearch 的基本 Query DSL 的 JSON 表述，使用花括号（`{}`）包含，JSON 的根键 (Root Key) 有且只能有一个，如 `match`、`geo_shape` 或 `bool` 等。

- Match 查询

  ```SQL
  SELECT * FROM es_table WHERE esquery(k4, '{
     "match": {
        "k4": "StarRocks on elasticsearch"
     }
  }');
  ```

- Geoshape 查询

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

- Boolean 查询

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

## 注意事项

- Elasticsearch 5.x 版本之前和之后的数据扫描方式不同，目前 StarRocks 只支持查询 Elasticsearch 5.x 版本之后的版本。
- 支持查询使用 HTTP Basic 认证的 Elasticsearch 集群。
- 一些通过 StarRocks 的查询会比直接请求 Elasticsearch 会慢很多，比如 `count()` 相关查询。这是因为 Elasticsearch 内部会直接读取满足条件的文档个数相关的元数据，不需要对真实的数据进行过滤操作，使得 `count()` 的速度非常快。
