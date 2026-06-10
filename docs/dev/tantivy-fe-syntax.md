# Tantivy FE syntax & DDL — Phase 1

This page documents the FE-side surface introduced by the
`add-tantivy-fe-ddl-sql` change. 
these features become end-to-end usable; until then the FE-side
validation is functional but BE-side query/build will report a missing
plugin for `imp_lib=tantivy`.

## SQL: MATCH_PHRASE

`MATCH_PHRASE` is a new keyword expression analogous to the existing
`MATCH / MATCH_ANY / MATCH_ALL`. It performs a phrase match; the
**slop** (max word distance) is encoded in the pattern string as a
trailing `~N` suffix.

```sql
-- Exact phrase, slop = 0
SELECT * FROM t WHERE body MATCH_PHRASE 'quick brown fox';

-- With slop = 3 (allows up to 3 intervening tokens)
SELECT * FROM t WHERE body MATCH_PHRASE 'quick fox ~3';

-- Negation
SELECT * FROM t WHERE NOT body MATCH_PHRASE 'quick fox';
```

### `~N` parsing rules

The trailing `~N` is identified by the regex `(?:^|\s+)~(\d+)\s*$`
applied on the pattern literal in FE `AstBuilder.visitMatchExpr`:

| Pattern | text | slop | Note |
|---|---|---|---|
| `'a b c'` | `a b c` | 0 | no marker |
| `'a b c ~3'` | `a b c` | 3 | basic case |
| `'a b c ~03'` | `a b c` | 3 | leading zero accepted |
| `'a b c~3'` | `a b c~3` | 0 | no whitespace before `~` → literal |
| `'a b c ~'` | `a b c ~` | 0 | `~` without digits → literal |
| `'~3'` | (empty) | 3 | rejected by analyzer (empty text) |
| `'a b c ~3 ~5'` | `a b c ~3` | 5 | only the trailing `~N` wins |
| `'a b c ~-1'` | `a b c ~-1` | 0 | minus is not a digit → literal |

Slop range: `[0, INT_MAX/2]`. Out-of-range values throw
`SemanticException` at analysis.

## DDL: imp_lib whitelist

`InvertedIndexImpType` now has three members:

```
CLUCENE   -- existing (shared-nothing only in shared-data mode)
BUILTIN   -- existing (default for shared-data when imp_lib unspecified)
TANTIVY   -- NEW (Phase 1: DUPLICATE_KEYS only)
```

```sql
-- Create a tantivy-backed GIN index on a duplicate-keys table:
CREATE TABLE t (
  id BIGINT NOT NULL,
  body VARCHAR(500) NOT NULL,
  INDEX gin_body(body) USING GIN ("imp_lib" = "tantivy", "parser" = "english")
) ENGINE = OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
```

### Validation rules (FE)

| Rule | Behavior |
|---|---|
| `enable_experimental_gin = false` | All GIN DDL rejected (existing) |
| Non-CHAR/VARCHAR column | Rejected (existing) |
| `imp_lib` outside `{clucene, builtin, tantivy}` | Rejected |
| `imp_lib = clucene` in shared-data mode | Rejected (existing) |
| `imp_lib = tantivy` in shared-data mode | Allowed |
| `imp_lib` unspecified in shared-data mode | Defaults to `BUILTIN` (unchanged from prior) |
| `imp_lib = tantivy` on non-DUP_KEYS table | **Rejected** (Phase 1 limit) |
| `dict_gram_num` set with `imp_lib = tantivy` | Rejected (existing rule: only builtin) |
| `support_phrase` / `support_bm25` set with non-tantivy | Rejected |

### Tantivy-specific parameters

Two new properties accepted on `imp_lib = tantivy` (whitelist-only in Phase 1):

| Param | Default | Effect (Phase 1) |
|---|---|---|
| `support_phrase` | `false` | Hint; tantivy BE always supports phrase regardless |
| `support_bm25` | `false` | Hint; Phase 2 will use this to enable fieldnorms |

## Wire / thrift

`Exprs.thrift` now has `TMatchExpr { i32 slop }` carried as
`TExprNode.match_expr` (field 58, optional). Backward compatible:

- New FE + old BE → BE ignores `match_expr` field; slop defaults to 0 → behavior identical to pre-change.
- Old FE + new BE → BE sees `match_expr` as unset; slop defaults to 0.

`TExprOpcode` gains `MATCH_PHRASE` (appended after existing
`MATCH/MATCH_ANY/MATCH_ALL`).

## Empty pattern rejection

`MATCH '' / MATCH_ANY '' / MATCH_ALL '' / MATCH_PHRASE ''` are all
rejected at FE analysis:

```
SemanticException: MATCH pattern must not be empty (operator: MATCH_PHRASE)
```

This guards against the NULL/empty-string semantic trap where, depending
on tokenizer, an empty pattern can match every NULL row (which is
encoded internally as a placeholder doc).

`MATCH_PHRASE '~3'` also triggers this rule: after the AstBuilder
strips off the `~N` slop marker, the remaining text is empty.

## Tests

- FE: `MatchPhraseParserTest`, `InvertedIndexUtilTantivyTest`,
  `InvertedIndexMatchPhrasePlanTest`
- SQL-tester: `test/sql/test_inverted_index/T/test_tantivy_ddl`,
  `test/sql/test_inverted_index/T/test_match_phrase`

The clucene path of `test_match_phrase` runs end-to-end today; the
tantivy variant becomes runnable after `add-tantivy-inverted-plugin` lands.
