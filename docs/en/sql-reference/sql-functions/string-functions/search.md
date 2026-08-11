---
displayed_sidebar: docs
description: "Search GIN-indexed columns with a full-text query DSL."
---

# search

`search()` performs full-text search on GIN-indexed columns. In its query string, you can combine field-qualified terms, Boolean operators, parentheses, and search clauses such as `ANY`, `ALL`, `IN`, and `EXACT`.

## Prerequisites

Each searched column must have a [full-text GIN index](../../../table_design/indexes/inverted_index.md). Before creating a GIN index, set the mutable FE configuration `enable_experimental_gin` to `true`:

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

The built-in `search()` function is disabled by default. Enable it after checking that existing UDFs and persisted definitions do not rely on an unqualified function with the same name:

```sql
ADMIN SET FRONTEND CONFIG ("enable_search_function" = "true");
```

The session variable `enable_gin_filter` must also remain enabled so search conditions can use the GIN index. It is enabled by default.

## Syntax

```sql
search('<dsl>' [, '<options_json>'])
```

Both arguments must be string literals. `search()` produces a Boolean predicate and is supported only as a Boolean leaf in the `WHERE` clause of a query block backed by one OLAP table. The table can be referenced directly or through simple derived tables that pass the searched columns through unchanged. A `search()` leaf can be combined with other predicates only through `AND`, `OR`, `NOT`, and parentheses. Nested query blocks are checked independently.

## Boolean syntax

Uppercase `AND`, `OR`, and `NOT` are explicit Boolean operators. The explicit-operator precedence is `NOT`, then `AND`, then `OR`; parentheses override it. Clauses separated only by whitespace form one implicit clause. `default_operator` combines the parts of that implicit clause.

An implicit clause is grouped before an adjacent explicit `AND` or `OR`. For example, `a b AND c d OR e` is parsed as `(implicit(a b) AND implicit(c d)) OR e`. With `default_operator: "or"`, this becomes `(a OR b) AND (c OR d) OR e`; with `and`, it becomes `(a AND b) AND (c AND d) OR e`. Explicit operators are never replaced by `default_operator`.

Lowercase `and`, `or`, and `not` are ordinary search terms.

`a NOT b` is also an implicit clause, so its outer combination depends on `default_operator`. Write `a AND NOT b` or `a OR NOT b` when that relationship should be explicit.

## Search clauses

| DSL clause | Query semantics |
| --- | --- |
| `field:term` | The tokenizer configured for the GIN index processes `term`; the query matches any resulting token by default, or all tokens when `default_operator` is `and` |
| `field:ANY(foo bar)` | Analyzes the query text and matches when at least one resulting token is present |
| `field:ALL(foo bar)` | Analyzes the query text and matches when every resulting token is present |
| `field:IN(foo bar)` | Matches either listed dictionary term without tokenizing the terms |
| `field:EXACT(foo)` | Matches the complete argument, including its internal whitespace, as one un-tokenized dictionary term |
| `field:foo*` | Uses one trailing `*` to match an un-tokenized term prefix |
| `field:*` | Matches rows where `field` is not `NULL`, after validating that the field has a GIN index |
| `field:(foo OR bar)` | The group inherits `field`; an explicitly qualified leaf inside the group can override it |
| `field:(foo bar)` | Evaluates `foo` and `bar` separately against the same field and combines them with `default_operator` |

`TERM`, `ANY`, and `ALL` queries are processed by the tokenizer configured for the bound GIN index. Raw `IN`, `EXACT`, and wildcard terms receive the index's case normalization but are not tokenized.

`ANY`, `ALL`, `IN`, and `EXACT` are case-insensitive and are recognized as clause names only when immediately followed by `(`. Otherwise, the same words can be used as field names or search terms.

## Options

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

- `default_field`: Binds clauses that do not name a field.
- `fields`: A non-empty array used to expand clauses that do not name a field.
- `type`: `best_fields` (default) or `cross_fields`. It is valid only with `fields`.
- `default_operator`: `or` (default) or `and`. It controls how tokens produced from an ordinary term and whitespace-separated parts of an implicit clause are combined. Explicit `AND` and `OR` are unaffected.

`default_field` and `fields` are mutually exclusive. A DSL containing only field-qualified clauses needs neither option. Explicitly qualified clauses keep their field when other clauses are expanded through `default_field` or `fields`.

`default_field: "body"` is semantically equivalent to `fields: ["body"]`. StarRocks treats both forms as the same single-field binding. `best_fields` and `cross_fields` can differ only when `fields` contains multiple fields and the DSL contains an unqualified clause.

StarRocks first determines the structure from explicit Boolean operators and parentheses, and uses `default_operator` to connect implicit clauses, forming the complete Boolean expression. It then applies `best_fields` or `cross_fields` to bind candidate fields to unqualified leaf clauses. Finally, the tokenizer configured for each bound field processes query text that requires tokenization. `ANY(...)`, `ALL(...)`, `IN(...)`, and `EXACT(...)` each participate in field binding as one leaf clause.

On the resulting Boolean expression, `best_fields` binds the complete structure to one configured field at a time and ORs the results. `cross_fields` expands each unqualified leaf clause independently across all configured fields. `cross_fields` requires compatible GIN implementation, tokenizer, and case-normalization settings. If every DSL leaf has an explicit field, `fields` and `type` do not participate in field binding or tokenizer-compatibility validation.

For `best_fields`, the complete surrounding Boolean structure is bound to one candidate field at a time. When a `NOT` node is encountered, its child is evaluated independently across all configured fields and the whole result is negated; that result then acts as a field-independent condition in every candidate-field branch. For example, with `fields: ["title", "body"]`, `foo AND (bar OR NOT baz)` is equivalent to `(title:foo AND (title:bar OR N)) OR (body:foo AND (body:bar OR N))`, where `N` is `NOT (title:baz OR body:baz)`. This preserves the same-field requirement for `foo` and `bar`; `N` follows SQL three-valued logic when either field is `NULL`.

## NULL handling

Search clauses follow SQL three-valued logic. A term, `ANY`, `ALL`, `IN`, `EXACT`, or wildcard query on a `NULL` field evaluates to `UNKNOWN`, so `NOT field:term` does not select rows where `field` is `NULL`. The existence clause `field:*` is different: it tests `field IS NOT NULL`, and `NOT field:*` therefore selects rows where the field is `NULL`.

## UDF compatibility

The mutable FE configuration `enable_search_function` controls the meaning of an unqualified `search(...)` call in query expressions. It is disabled by default so an upgrade does not take over existing unqualified `search` UDF calls. Before enabling it, audit same-named UDFs and persisted View or materialized view definitions; qualify calls that must continue to bind to a UDF as `db.search(...)`. When enabled, unqualified calls are reserved for the built-in search function in every expression context, and unsupported positions or signatures report a search-specific error. When disabled, unqualified calls use normal function resolution. A qualified call always uses normal function resolution. A prepared statement that was already bound to a UDF while the setting was disabled keeps that UDF binding if the setting changes later.

## Limitations

- Every referenced field must be a CHAR, VARCHAR, or STRING column with a GIN index.
- DSL field names can contain ASCII letters, digits, and underscores, with an optional table or alias qualifier in the form `table_alias.column`. Longer qualified names and names that require SQL quoting are not supported in this version.
- The `FROM` clause of a query block that contains `search()` must resolve to one OLAP table, either directly or through derived tables that only project and filter rows. Every searched field must be an unchanged column reference in each derived table. Computed or masked search fields, Views, CTEs, joins, external tables, table functions, aggregation, `DISTINCT`, window functions, and `LIMIT` are not supported in this path.
- Prepared statements cannot use the built-in `search()` function.
- Materialized view definitions cannot use `search()`.
- Only the query syntax described on this page is supported. Phrase, regular-expression, range, fuzzy, single-character wildcard, and escape queries are not supported. `^`, a leading `+` or `-` in a term, `%`, `&&`, `||`, `!`, `NESTED`, a leading, middle, or repeated `*`, and `*` inside `ANY`, `ALL`, `IN`, or `EXACT` are rejected.
- The DSL is limited to 1,048,576 UTF-16 code units and 200 nesting levels. Options are limited to 4,096 UTF-16 code units, and each `search()` query is limited to 10,000 predicate nodes.

## Examples

The following examples use this table and data:

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

### Search a specified field

Search the `title` column for `machine`:

```sql
SELECT id FROM documents
WHERE search('title:machine')
ORDER BY id;
-- 1, 4
```

### Match any or all tokens

`ANY` matches at least one analyzed token, whereas `ALL` requires every analyzed token:

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

### Match dictionary terms

Use `IN` to match one of several dictionary terms. Use `EXACT` to treat the complete argument as one dictionary term:

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

### Combine Boolean conditions

`AND` has higher precedence than `OR`. Parentheses can change the evaluation order, and `NOT` negates a condition:

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

### Bind unqualified clauses to a default field

Use `default_field` for clauses that do not name a field. `default_operator` controls how whitespace-separated clauses are combined:

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

### Search multiple fields

For the unqualified conditions in this example, `best_fields` requires all conditions to match one candidate field. `cross_fields` allows different conditions to match different candidate fields:

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

### Use wildcard and existence queries

Use a trailing `*` to match a term prefix. Use `field:*` to match rows in which the field is not `NULL`:

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

### Combine with SQL predicates

You can combine `search()` with ordinary SQL predicates through Boolean operators:

```sql
SELECT id
FROM documents
WHERE id > 1 AND search('title:machine')
ORDER BY id;
-- 4
```

### Rank results by BM25 score

To sort matching rows by BM25 relevance, create a builtin GIN index with `index_options = 'DOCS_AND_FREQS'` and order by `score()`. BM25 ranking has the following additional requirements:

- The query block must directly scan one OLAP table and cannot use aggregation, `DISTINCT`, or window functions.
- `WHERE` must contain exactly one full-text search leaf on one column as a top-level `AND` condition. Multi-field expansion, `IN(...)` with multiple terms, implicit multi-clause input, and a search condition nested in `OR` are not supported for scoring.
- `score()` must be the only `ORDER BY` key, either directly or through its SELECT alias or ordinal, and the query must have a positive `LIMIT`.

```sql
SELECT id FROM documents
WHERE search('bm25_text:ANY(quick fox)')
ORDER BY score() DESC
LIMIT 10;
-- 3, 1
```
