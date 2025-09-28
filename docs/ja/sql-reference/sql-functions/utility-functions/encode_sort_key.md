---
displayed_sidebar: docs
---

# encode_sort_key

複数の異種入力列から順序を保持する複合バイナリキーを作成します。この関数は、辞書順で比較した際に元のソート順序を維持する効率的なソートキーを作成するために不可欠です。

## 構文

```SQL
encode_sort_key(column1, column2, ..., columnN)
```

## パラメータ

- `column1, column2, ..., columnN`: サポートされている任意のデータ型の1つ以上の列。この関数は可変数の引数を受け入れます。

## 戻り値

複合ソートキーを表すVARBINARY型の値を返します。

## サポートされるデータ型

### サポートされる型

| データ型 | 説明 |
|----------|------|
| `TINYINT` | 8ビット符号付き整数 |
| `SMALLINT` | 16ビット符号付き整数 |
| `INT` | 32ビット符号付き整数 |
| `BIGINT` | 64ビット符号付き整数 |
| `LARGEINT` | 128ビット符号付き整数 |
| `FLOAT` | 32ビット浮動小数点数 |
| `DOUBLE` | 64ビット浮動小数点数 |
| `VARCHAR` | 可変長文字列 |
| `CHAR` | 固定長文字列 |
| `DATE` | 日付値 |
| `DATETIME` | 日時値 |
| `TIMESTAMP` | タイムスタンプ値 |

### サポートされない型

以下の複雑な型はサポートされておらず、エラーが返されます：

- `JSON`
- `ARRAY`
- `MAP`
- `STRUCT`
- `HLL`
- `BITMAP`
- `PERCENTILE`

## エンコーディング戦略

この関数は、結果のバイナリキーの辞書順比較が元のソート順序を保持するように、異なるデータ型に対して異なるエンコーディング戦略を使用します：

- **整数型**: ビッグエンディアンバイト順序、符号付き型の符号ビットを反転
- **浮動小数点型**: 正しいソート順序を保証するカスタムエンコーディング
- **文字列型**: 非最後フィールドで0x00バイトエスケープと0x00 0x00終端子を使用する特殊エンコーディング
- **日時型**: 内部整数表現を整数値としてエンコード

### NULL処理

- 各列の各行にNULLマーカー（NULLは0x00、非NULLは0x01）が付与される
- 一貫したエンコーディングを保証するために、非NULL列でもNULLマーカーが追加される
- 列を区切るために区切りバイト（0x00）が使用される（最後の列の後を除く）

## 例

### 生成ソートキー列

```sql
CREATE TABLE user_analytics (
    user_id INT,
    region VARCHAR(50),
    score DOUBLE,
    created_date DATE,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(region, score, created_date)
    )
) ORDER BY (sort_key);
```

### JSONフィールド抽出

```sql
CREATE TABLE json_data (
    id INT,
    json_content JSON,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(
            get_json_int(json_content, '$.priority'),
            get_json_string(json_content, '$.category'),
            get_json_double(json_content, '$.score')
        )
    )
) ORDER BY (sort_key);
```

## 制限

### データ型制限

JSON、ARRAY、MAP、STRUCTなどの複雑な型は直接エンコードできません。プリミティブ値を取得するにはJSON抽出関数を使用してください：

```sql
-- 代わりに: encode_sort_key(json_col)
-- 使用: encode_sort_key(get_json_int(json_col, '$.field1'), get_json_string(json_col, '$.field2'))
```

### パフォーマンス考慮事項

- `encode_sort_key`の各呼び出しで、すべての入力列のエンコーディングが必要
- バイナリキーは元のデータよりも大幅に大きくなる可能性がある
- エンコーディングの繰り返しを避けるために生成列を使用

### キーサイズ制限

- 列数を合理的に保つ（通常< 10）
- 可能な限り短い文字列列を優先する
- 非常に長い文字列にはハッシュ関数の使用を検討する

### バージョン互換性

StarRocksバージョン間で内部エンコーディング形式が変更される可能性があります。長期永続化のためにエンコードされたキーを保存しないでください。
