---
displayed_sidebar: docs
description: "全文検索クエリ DSL を使用して、GIN インデックス付きカラムを検索します。"
---

# search

`search()` は、GIN インデックス付きテキストカラムで全文検索を実行します。クエリ文字列では、フィールド修飾された検索語、Boolean 演算子、括弧、および `ANY`、`ALL`、`IN`、`EXACT` などの検索句を組み合わせることができます。

## 前提条件

検索する各カラムには[全文 GIN インデックス](../../../table_design/indexes/inverted_index.md)が必要です。GIN インデックスを作成する前に、変更可能な FE 設定 `enable_experimental_gin` を `true` に設定します。

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

組み込み `search()` 関数はデフォルトで無効です。既存の UDF と永続化された定義が、同名の未修飾関数に依存していないことを確認してから有効にします。

```sql
ADMIN SET FRONTEND CONFIG ("enable_search_function" = "true");
```

検索条件で GIN インデックスを使用できるように、セッション変数 `enable_gin_filter` も有効にしておく必要があります。この変数はデフォルトで有効です。

## 構文

```sql
search('<dsl>' [, '<options_json>'])
```

両方の引数は文字列リテラルでなければなりません。`search()` は Boolean 述語を生成し、1 つの OLAP テーブルを基にするクエリブロックの `WHERE` 句で、Boolean リーフとしてのみ使用できます。テーブルは直接参照するか、検索対象カラムを変更せずに渡す単純な派生テーブルを介して参照できます。`search()` リーフは、`AND`、`OR`、`NOT`、および括弧だけを使用して他の述語と組み合わせることができます。ネストされたクエリブロックは個別に検査されます。

## Boolean 構文

大文字の `AND`、`OR`、`NOT` は明示的な Boolean 演算子です。明示的な演算子の優先順位は `NOT`、`AND`、`OR` の順で、括弧によって上書きできます。空白だけで区切られた隣接句は 1 つの暗黙句となり、その各部分は `default_operator` で結合されます。

暗黙句は、隣接する明示的な `AND` または `OR` より先にグループ化されます。たとえば `a b AND c d OR e` は `(implicit(a b) AND implicit(c d)) OR e` として解析されます。`default_operator` が `or` なら `(a OR b) AND (c OR d) OR e`、`and` なら `(a AND b) AND (c AND d) OR e` となります。`default_operator` が明示的な演算子を置き換えることはありません。

小文字の `and`、`or`、`not` は通常の検索語です。

`a NOT b` も暗黙句であるため、その外側の結合は `default_operator` に依存します。関係を明示する場合は、`a AND NOT b` または `a OR NOT b` と記述してください。

## 検索句

| DSL 句 | クエリの意味 |
| --- | --- |
| `field:term` | GIN インデックスに設定されたトークナイザーで `term` を処理し、デフォルトでは得られたトークンのいずれか、`default_operator` が `and` の場合はすべてのトークンに一致する行を検索 |
| `field:ANY(foo bar)` | クエリテキストを解析し、得られたトークンの少なくとも 1 つが存在する行を検索 |
| `field:ALL(foo bar)` | クエリテキストを解析し、得られたすべてのトークンが存在する行を検索 |
| `field:IN(foo bar)` | 列挙した辞書 term のいずれかに一致する行を検索。各 term はトークン化されない |
| `field:EXACT(foo)` | 内部の空白を含む引数全体を、トークン化されない 1 つの辞書 term として検索 |
| `field:foo*` | 末尾の 1 つの `*` を使用して、トークン化されない term のプレフィックスを検索 |
| `field:*` | フィールドに GIN インデックスがあることを検証し、その値が `NULL` でない行を検索 |
| `field:(foo OR bar)` | グループ内の条件は `field` を継承し、グループ内で明示的に修飾したリーフはそのフィールドを上書き可能 |
| `field:(foo bar)` | 同じフィールドに対して `foo` と `bar` を個別に評価し、`default_operator` で結合 |

`TERM`、`ANY`、`ALL` は、バインドされた GIN インデックスに設定されたトークナイザーで処理されます。`IN`、`EXACT`、ワイルドカードの生の term にはインデックスの大文字小文字正規化が適用されますが、トークン化はされません。

`ANY`、`ALL`、`IN`、`EXACT` は大文字小文字を区別せず、直後に `(` がある場合だけ句名として認識されます。それ以外では、同じ語をフィールド名または検索語として使用できます。

## オプション

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

- `default_field`: フィールドを指定しない句をバインドします。
- `fields`: フィールドを指定しない句を展開する空でない配列です。
- `type`: `best_fields`（デフォルト）または `cross_fields`。`fields` と一緒にのみ使用できます。
- `default_operator`: `or`（デフォルト）または `and`。通常の term から得られたトークンと、暗黙句内で空白区切りされた部分の組み合わせを制御します。明示的な `AND` と `OR` には影響しません。

`default_field` と `fields` は同時に指定できません。すべての句がフィールドを明示している場合は、どちらも不要です。明示的なフィールド句は、他の句が `default_field` または `fields` で展開されてもフィールドを維持します。

`default_field: "body"` と `fields: ["body"]` は意味的に同等で、StarRocks は両方を同じ単一フィールドバインディングとして扱います。`best_fields` と `cross_fields` が異なる可能性があるのは、`fields` に複数のフィールドが含まれ、DSL に未修飾の句がある場合だけです。

StarRocks はまず明示的な Boolean 演算子と括弧から構造を決定し、`default_operator` で暗黙句を結合して完全な Boolean 式を作成します。次に、`best_fields` または `cross_fields` に従って未修飾のリーフ句へ候補フィールドをバインドします。最後に、各バインド先フィールドに設定されたトークナイザーが、トークナイズを必要とするクエリテキストを処理します。`ANY(...)`、`ALL(...)`、`IN(...)`、`EXACT(...)` は、それぞれ 1 つのリーフ句としてフィールドバインディングに参加します。

作成された Boolean 式に対して、`best_fields` は構造全体を一度に 1 つの設定フィールドへバインドし、結果を OR で結合します。`cross_fields` は、各未修飾リーフ句をすべての設定フィールドへ個別に展開します。`cross_fields` では、GIN 実装、トークナイザー、大文字小文字正規化設定に互換性が必要です。DSL のすべてのリーフでフィールドが明示されている場合、`fields` と `type` はフィールドバインディングやトークナイズ設定の互換性検査には使用されません。

`best_fields` では、周囲の完全な Boolean 構造を候補フィールドごとに 1 つずつバインドします。`NOT` ノードに到達すると、その子ツリーをすべての設定フィールドに対して個別に評価してから結果全体を否定し、その結果を各候補フィールド分岐でフィールド非依存の条件として使用します。たとえば `fields: ["title", "body"]` の場合、`foo AND (bar OR NOT baz)` は `(title:foo AND (title:bar OR N)) OR (body:foo AND (body:bar OR N))` と等価で、`N` は `NOT (title:baz OR body:baz)` です。これにより `foo` と `bar` の同一フィールド要件が維持され、いずれかのフィールドが `NULL` の場合、`N` は SQL の三値論理に従います。

## NULL の処理

検索句は SQL の三値論理に従います。値が `NULL` のフィールドに対する通常の term、`ANY`、`ALL`、`IN`、`EXACT`、またはワイルドカードクエリは `UNKNOWN` になるため、`NOT field:term` は `field` が `NULL` の行を選択しません。存在句 `field:*` は異なり、`field IS NOT NULL` を検査するため、`NOT field:*` はフィールドが `NULL` の行を選択します。

## UDF 互換性

動的 FE 設定 `enable_search_function` は、クエリ式内の未修飾 `search(...)` 呼び出しの意味を制御します。アップグレード後に既存の未修飾 `search` UDF 呼び出しを引き継がないよう、デフォルトでは無効です。有効にする前に、同名の UDF と永続化された View またはマテリアライズドビュー定義を確認し、引き続き UDF にバインドする呼び出しは `db.search(...)` と記述してください。有効にすると、すべての式コンテキストにある未修飾の呼び出しは組み込み search 関数用に予約され、未対応の位置やシグネチャは search 固有のエラーになります。無効にすると、未修飾の呼び出しは通常の関数解決に渡されます。データベース修飾された呼び出しは常に通常の関数解決を使用します。設定が無効なときに Prepared Statement が UDF にバインド済みの場合、その後設定を変更しても UDF バインディングは維持されます。

## 制限

- 参照する各フィールドは、GIN インデックスを持つ CHAR、VARCHAR、STRING カラムでなければなりません。
- DSL フィールド名には ASCII 英数字とアンダースコアを使用でき、`table_alias.column` 形式でテーブルまたはエイリアスを任意に修飾できます。このバージョンでは、より長い修飾名や SQL の引用符が必要な名前はサポートされません。
- `search()` を含むクエリブロックの `FROM` 句は、直接、または射影とフィルタリングだけを行う派生テーブルを介して、1 つの OLAP テーブルに解決される必要があります。検索対象の各フィールドは、各派生テーブルで変更されていないカラム参照でなければなりません。この経路では、計算またはマスキングされた検索フィールド、View、CTE、JOIN、外部テーブル、テーブル関数、集約、`DISTINCT`、ウィンドウ関数、`LIMIT` はサポートされません。
- Prepared Statement では組み込み `search()` 関数を使用できません。
- マテリアライズドビュー定義では `search()` を使用できません。
- このページに記載されたクエリ構文のみをサポートします。フレーズ、正規表現、範囲、あいまい検索、1 文字ワイルドカード、エスケープを使用するクエリはサポートされません。`^`、term の先頭にある `+` または `-`、`%`、`&&`、`||`、`!`、`NESTED`、term の先頭または途中にある `*`、重複した `*`、および `ANY`、`ALL`、`IN`、`EXACT` 内の `*` は拒否されます。
- DSL は最大 1,048,576 UTF-16 コード単位、ネストは最大 200 レベルです。options は最大 4,096 UTF-16 コード単位で、各 `search()` クエリは最大 10,000 述語ノードです。

## 例

以下の例では、次のテーブルとデータを使用します。

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

### 指定したフィールドを検索する

`title` 列で `machine` を検索します。

```sql
SELECT id FROM documents
WHERE search('title:machine')
ORDER BY id;
-- 1, 4
```

### 任意またはすべてのトークンを照合する

`ANY` は解析されたトークンのうち少なくとも 1 つ、`ALL` はすべてのトークンに一致することを要求します。

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

### 辞書 term を照合する

`IN` は複数の辞書 term のいずれかを照合します。`EXACT` は引数全体を 1 つの辞書 term として扱います。

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

### Boolean 条件を組み合わせる

`AND` は `OR` より優先されます。括弧で評価順序を変更でき、`NOT` で条件を否定できます。

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

### フィールド未指定の句をデフォルトフィールドにバインドする

`default_field` はフィールドを指定していない句にフィールドをバインドします。`default_operator` は空白だけで区切られた句の結合方法を制御します。

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

### 複数のフィールドを検索する

この例のフィールド未指定の条件について、`best_fields` では、すべての条件が同じ候補フィールドに一致する必要があります。`cross_fields` では、異なる条件を異なる候補フィールドで照合できます。

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

### ワイルドカードクエリと存在クエリを使用する

term の末尾に `*` を付けて term のプレフィックスを照合します。`field:*` はフィールドが `NULL` でない行を照合します。

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

### SQL 述語と組み合わせる

Boolean 演算子を使用して `search()` と通常の SQL 述語を組み合わせることができます。

```sql
SELECT id
FROM documents
WHERE id > 1 AND search('title:machine')
ORDER BY id;
-- 4
```

### BM25 スコアで結果を並べ替える

BM25 関連度で一致した行を並べ替えるには、builtin GIN インデックスを `index_options = 'DOCS_AND_FREQS'` で作成し、`score()` で並べ替えます。さらに、次の条件を満たす必要があります。

- クエリブロックは 1 つの OLAP テーブルを直接スキャンし、集約、`DISTINCT`、ウィンドウ関数を含めることはできません。
- `WHERE` には、1 つのカラムを対象とする全文検索リーフを 1 つだけ含め、その条件をトップレベルの `AND` に配置する必要があります。複数フィールドへの展開、複数の term を含む `IN(...)`、複数の暗黙句を含む入力、`OR` 内の検索条件はスコアリングに使用できません。
- `score()` は唯一の `ORDER BY` キーでなければなりません。直接指定するほか、SELECT の別名または序数を使用できます。また、クエリには正の `LIMIT` が必要です。

```sql
SELECT id FROM documents
WHERE search('bm25_text:ANY(quick fox)')
ORDER BY score() DESC
LIMIT 10;
-- 3, 1
```
