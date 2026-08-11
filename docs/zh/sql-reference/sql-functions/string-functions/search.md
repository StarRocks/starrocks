---
displayed_sidebar: docs
description: "使用 search() 函数在具有 GIN 索引的文本列中执行全文检索。"
---

# search

`search()` 用于在具有 GIN 索引的文本列上执行全文检索。您可以在查询字符串中组合字段限定词、布尔操作符、括号，以及 `ANY`、`ALL`、`IN` 和 `EXACT` 等搜索子句。

## 前提条件

每个参与搜索的列都必须具有[全文倒排索引](../../../table_design/indexes/inverted_index.md)。创建 GIN 索引前，需要将动态 FE 配置 `enable_experimental_gin` 设置为 `true`：

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

内置 `search()` 函数默认关闭。确认已有 UDF 和持久化定义没有依赖同名的未限定函数后，可以启用该功能：

```sql
ADMIN SET FRONTEND CONFIG ("enable_search_function" = "true");
```

同时需要保持会话变量 `enable_gin_filter` 为启用状态，使搜索条件能够使用 GIN 索引。该变量默认启用。

## 语法

```sql
search('<dsl>' [, '<options_json>'])
```

两个参数都必须是字符串字面量。`search()` 生成布尔谓词，只能作为某个查询块 `WHERE` 子句中的布尔叶子使用，并且该查询块必须基于一个 OLAP 表。该表既可以被直接引用，也可以位于仅透传搜索列的简单派生表之后。`search()` 叶子只能通过 `AND`、`OR`、`NOT` 和括号与其他谓词组合。嵌套查询块会独立检查。

## 布尔语法

大写 `AND`、`OR` 和 `NOT` 是显式布尔操作符。显式操作符的优先级依次为 `NOT`、`AND`、`OR`，括号可以覆盖默认优先级。仅以空白分隔的相邻子句构成一个隐式子句，其中各部分由 `default_operator` 连接。

隐式子句会先于相邻的显式 `AND` 或 `OR` 成组。例如，`a b AND c d OR e` 解析为 `(implicit(a b) AND implicit(c d)) OR e`。当 `default_operator` 为 `or` 时，其含义为 `(a OR b) AND (c OR d) OR e`；为 `and` 时，其含义为 `(a AND b) AND (c AND d) OR e`。`default_operator` 不会替换显式操作符。

小写 `and`、`or` 和 `not` 被视为普通搜索词。

`a NOT b` 同样属于隐式子句，因此其外层组合取决于 `default_operator`。如果需要明确表达该关系，请写成 `a AND NOT b` 或 `a OR NOT b`。

## 搜索子句

| DSL 子句 | 查询语义 |
| --- | --- |
| `field:term` | 使用 GIN 索引配置的分词器处理 `term`；默认匹配任意一个分词结果，当 `default_operator` 为 `and` 时要求匹配全部分词结果 |
| `field:ANY(foo bar)` | 对查询文本分词，并在至少一个分词结果存在时匹配 |
| `field:ALL(foo bar)` | 对查询文本分词，并在所有分词结果均存在时匹配 |
| `field:IN(foo bar)` | 匹配任意一个列出的词典 term，各 term 不经过分词 |
| `field:EXACT(foo)` | 将包含内部空白的完整参数作为一个不经过分词的词典 term 进行匹配 |
| `field:foo*` | 使用一个位于末尾的 `*` 匹配不经过分词的 term 前缀 |
| `field:*` | 验证字段具有 GIN 索引后，匹配该字段不为 `NULL` 的行 |
| `field:(foo OR bar)` | 组内条件继承 `field`；组内显式限定字段的叶子可以覆盖该字段 |
| `field:(foo bar)` | 分别在同一字段上计算 `foo` 和 `bar`，再用 `default_operator` 连接 |

`TERM`、`ANY` 和 `ALL` 查询会经过绑定字段的 GIN 分词器。`IN`、`EXACT` 和通配符中的原始 term 会应用索引的大小写归一化规则，但不会经过分词。

`ANY`、`ALL`、`IN` 和 `EXACT` 不区分大小写，并且只有在紧跟 `(` 时才会被识别为子句名；否则，同名文本仍可作为字段名或搜索词。

## 选项

```json
{
  "default_field": "body",
  "default_operator": "or"
}
```

```json
{
  "fields": ["title", "body"],
  "type": "cross_fields",
  "default_operator": "and"
}
```

- `default_field`：绑定未指定字段的子句。
- `fields`：用于展开未指定字段子句的非空字段数组。
- `type`：可选值为 `best_fields`（默认）或 `cross_fields`，只能与 `fields` 一起使用。
- `default_operator`：可选值为 `or`（默认）或 `and`，控制普通 term 的分词结果以及隐式子句中空白分隔部分的组合方式，不影响显式 `AND` 和 `OR`。

`default_field` 和 `fields` 互斥。如果 DSL 中的所有子句都显式指定字段，则两者都不需要。显式字段子句不会因其他子句通过 `default_field` 或 `fields` 展开而改变字段。

`default_field: "body"` 与 `fields: ["body"]` 语义等价，StarRocks 会将两种形式都作为相同的单字段绑定处理。只有当 `fields` 包含多个字段且 DSL 中存在未限定字段的子句时，`best_fields` 和 `cross_fields` 才可能存在区别。

StarRocks 会先按照显式布尔操作符和括号确定结构，并使用 `default_operator` 连接隐式子句，形成完整的布尔表达式；再按照 `best_fields` 或 `cross_fields` 为未指定字段的叶子条件绑定候选字段；最后由各绑定字段配置的分词器处理需要分词的查询文本。`ANY(...)`、`ALL(...)`、`IN(...)` 和 `EXACT(...)` 均作为单个叶子条件参与字段绑定。

对于形成后的布尔表达式，`best_fields` 会依次把完整结构绑定到一个配置字段，再用 OR 合并结果；`cross_fields` 则把每个未指定字段的叶子条件分别展开到所有配置字段。`cross_fields` 要求相关字段的 GIN 实现、分词器和大小写归一化设置兼容。如果 DSL 中的每个叶子都已显式指定字段，则 `fields` 和 `type` 不参与字段绑定或分词配置兼容性校验。

对于 `best_fields`，外围的完整布尔结构每次只绑定到一个候选字段。遇到 `NOT` 节点时，其子树会分别在所有配置字段上求值，再对整个结果取反；该结果作为与候选字段无关的条件放入每个字段分支。例如，使用 `fields: ["title", "body"]` 时，`foo AND (bar OR NOT baz)` 等价于 `(title:foo AND (title:bar OR N)) OR (body:foo AND (body:bar OR N))`，其中 `N` 为 `NOT (title:baz OR body:baz)`。因此 `foo` 和 `bar` 仍必须在同一个候选字段中满足；任一字段为 `NULL` 时，`N` 遵循 SQL 三值逻辑。

## NULL 处理

搜索子句遵循 SQL 三值逻辑。在值为 `NULL` 的字段上执行普通 term、`ANY`、`ALL`、`IN`、`EXACT` 或通配查询时，结果为 `UNKNOWN`，因此 `NOT field:term` 不会选出 `field` 为 `NULL` 的行。存在性子句 `field:*` 不同：它检查 `field IS NOT NULL`，所以 `NOT field:*` 会选出该字段为 `NULL` 的行。

## UDF 兼容性

动态 FE 配置 `enable_search_function` 决定查询表达式中未限定 `search(...)` 调用的含义。该配置默认关闭，避免升级后接管已有的未限定 `search` UDF 调用。启用前，请检查同名 UDF 和持久化 View 或物化视图定义；必须继续绑定 UDF 的调用应写成 `db.search(...)`。启用后，所有表达式位置中的未限定调用均保留给内置 search 函数，不支持的位置或签名会返回 search 专属错误。禁用后，未限定调用交由常规函数解析流程处理；数据库限定调用始终使用常规函数解析流程。如果 Prepared Statement 在配置关闭时已经绑定到 UDF，之后切换配置不会改变该 UDF 绑定。

## 限制

- 每个引用字段都必须是具有 GIN 索引的 CHAR、VARCHAR 或 STRING 列。
- DSL 字段名只能包含 ASCII 字母、数字和下划线，并可按 `table_alias.column` 形式添加表名或别名限定。本版本不支持更长的限定名称，也不支持必须通过 SQL 引号引用的字段名。
- 包含 `search()` 的查询块，其 `FROM` 子句必须直接或通过仅执行投影和过滤的派生表解析到一个 OLAP 表。每个搜索字段在各层派生表中都必须是未经转换的列引用。此路径不支持经过计算或掩码处理的搜索字段，也不支持 View、CTE、JOIN、外表、表函数、聚合、`DISTINCT`、窗口函数或 `LIMIT`。
- Prepared Statement 不能使用内置 `search()` 函数。
- 物化视图定义不能使用 `search()`。
- 仅支持本文列出的查询语法。短语、正则、范围、模糊、单字符通配和转义查询暂不支持。`^`、位于 term 开头的 `+` 或 `-`、`%`、`&&`、`||`、`!`、`NESTED`、位于 term 开头或中间的 `*`、重复的 `*`，以及 `ANY`、`ALL`、`IN` 或 `EXACT` 内的 `*` 会被拒绝。
- DSL 最长为 1,048,576 个 UTF-16 code unit，最多嵌套 200 层；options 最长为 4,096 个 UTF-16 code unit，每个 `search()` 查询最多包含 10,000 个谓词节点。

## 示例

以下示例使用如下表和数据：

```sql
CREATE TABLE documents (
    id BIGINT,
    title VARCHAR(100),
    body VARCHAR(100),
    category VARCHAR(100),
    exact_text VARCHAR(100),
    bm25_text VARCHAR(100),
    INDEX idx_title (title) USING GIN ("imp_lib" = "builtin", "parser" = "english"),
    INDEX idx_body (body) USING GIN ("imp_lib" = "builtin", "parser" = "english"),
    INDEX idx_category (category) USING GIN ("imp_lib" = "builtin", "parser" = "english"),
    INDEX idx_exact (exact_text) USING GIN ("imp_lib" = "builtin", "parser" = "none"),
    INDEX idx_bm25 (bm25_text) USING GIN (
        "imp_lib" = "builtin",
        "parser" = "english",
        "index_options" = "DOCS_AND_FREQS"
    )
)
ENGINE = OLAP
DUPLICATE KEY (id)
DISTRIBUTED BY HASH (id);

INSERT INTO documents VALUES
    (1, 'Machine Learning', 'Cloud Database', 'Tech', 'Hello World', 'the quick brown fox'),
    (2, 'Cloud Native', 'Machine Learning', 'Archive', 'hello world', 'the lazy dog'),
    (3, 'Other', 'Nothing', NULL, 'Other', 'quick quick fox'),
    (4, 'Machine Cloud', 'Database', 'Tech', 'Machine Cloud', 'a slow green turtle');
```

### 搜索指定字段

在 `title` 列中搜索 `machine`：

```sql
SELECT id FROM documents
WHERE search('title:machine')
ORDER BY id;
-- 1, 4
```

### 匹配任意或全部分词结果

`ANY` 要求至少匹配一个分词结果，`ALL` 则要求匹配全部分词结果：

```sql
SELECT id FROM documents
WHERE search('title:ANY(cloud other)')
ORDER BY id;
-- 2, 3, 4

SELECT id FROM documents
WHERE search('body:ALL(machine learning)')
ORDER BY id;
-- 2
```

### 匹配词典 term

使用 `IN` 匹配多个词典 term 中的任意一个；使用 `EXACT` 将完整参数作为一个词典 term：

```sql
SELECT id FROM documents
WHERE search('category:IN(Tech Archive)')
ORDER BY id;
-- 1, 2, 4

SELECT id FROM documents
WHERE search('exact_text:EXACT(Hello World)')
ORDER BY id;
-- 1
```

### 组合布尔条件

`AND` 的优先级高于 `OR`。括号可以改变计算顺序，`NOT` 用于对条件取反：

```sql
SELECT id FROM documents
WHERE search('title:machine OR body:machine AND category:archive')
ORDER BY id;
-- 1, 2, 4

SELECT id FROM documents
WHERE search('(title:machine OR body:machine) AND category:archive')
ORDER BY id;
-- 2

SELECT id FROM documents
WHERE search('title:machine AND NOT body:learning')
ORDER BY id;
-- 1, 4
```

### 将未指定字段的子句绑定到默认字段

使用 `default_field` 为未指定字段的子句绑定字段。`default_operator` 控制如何连接仅以空白分隔的子句：

```sql
SELECT id FROM documents
WHERE search(
    'machine cloud',
    '{"default_field":"title","default_operator":"and"}'
)
ORDER BY id;
-- 4

SELECT id FROM documents
WHERE search(
    'category:tech AND machine',
    '{"default_field":"title"}'
)
ORDER BY id;
-- 1, 4
```

### 搜索多个字段

对于本例中的未指定字段条件，使用 `best_fields` 时，所有条件必须在同一个候选字段中匹配；使用 `cross_fields` 时，不同条件可以在不同候选字段中匹配：

```sql
SELECT id FROM documents
WHERE search(
    'machine AND cloud',
    '{"fields":["title","body"],"type":"best_fields"}'
)
ORDER BY id;
-- 4

SELECT id FROM documents
WHERE search(
    'machine AND cloud',
    '{"fields":["title","body"],"type":"cross_fields"}'
)
ORDER BY id;
-- 1, 2, 4
```

### 使用通配和存在性查询

在 term 末尾使用 `*` 匹配 term 前缀；使用 `field:*` 匹配该字段不为 `NULL` 的行：

```sql
SELECT id FROM documents
WHERE search('title:Mach*')
ORDER BY id;
-- 1, 4

SELECT id FROM documents
WHERE search('category:*')
ORDER BY id;
-- 1, 2, 4
```

### 与 SQL 谓词组合

可以通过布尔操作符将 `search()` 与普通 SQL 谓词组合：

```sql
SELECT id
FROM documents
WHERE id > 1 AND search('title:machine')
ORDER BY id;
-- 4
```

### 按 BM25 相关性排序

如需按 BM25 相关性对匹配结果排序，请使用 `index_options = 'DOCS_AND_FREQS'` 创建 builtin GIN 索引，并按 `score()` 排序。此外还必须满足以下条件：

- 查询块必须直接扫描一个 OLAP 表，并且不能包含聚合、`DISTINCT` 或窗口函数。
- `WHERE` 中必须只有一个作用于单列的全文搜索叶子，并且该条件必须位于顶层 `AND` 中。多字段展开、包含多个 term 的 `IN(...)`、包含多个隐式子句的输入，以及位于 `OR` 内的搜索条件均不能用于相关性排序。
- `score()` 必须是唯一的 `ORDER BY` 键，可以直接使用，也可以使用其 SELECT 别名或序号；查询还必须具有正数 `LIMIT`。

```sql
SELECT id FROM documents
WHERE search('bm25_text:ANY(quick fox)')
ORDER BY score() DESC
LIMIT 10;
-- 3, 1
```
