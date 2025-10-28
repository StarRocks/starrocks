---
displayed_sidebar: docs
---

# format_bytes

バイト数を適切な単位（B, KB, MB, GB, TB, PB, EB）で人間が読みやすい文字列に変換します。

この関数は、ファイルサイズ、テーブルサイズ、メモリ使用量、その他のストレージ関連のメトリクスをユーザーフレンドリーな形式で表示するのに便利です。1024ベースの計算（バイナリプレフィックス）を使用しますが、単位は簡単で親しみやすいようにKB, MB, GBとして表示されます。

この関数はv3.4からサポートされています。

## 構文

```Haskell
VARCHAR format_bytes(BIGINT bytes)
```

## パラメータ

- `bytes`: フォーマットするバイト数。BIGINTがサポートされています。非負の整数値である必要があります。

## 戻り値

適切な単位でフォーマットされたバイトサイズを表すVARCHAR値を返します。

- 1024未満の値の場合: 正確な数値と"B"（バイト）を返します
- より大きな値の場合: 2桁の小数点を持つフォーマットされた文字列と適切な単位（KB, MB, GB, TB, PB, EB）を返します
- 入力が負またはNULLの場合はNULLを返します

## 使用上の注意

- 関数は内部的に1024ベース（バイナリ）の計算を使用します（1 KB = 1024バイト、1 MB = 1024²バイトなど）
- 単位はユーザーの親しみやすさのためにKB, MB, GB, TB, PB, EBとして表示されますが、バイナリプレフィックス（KiB, MiB, GiBなど）を表します
- 1 KB以上の値は正確に2桁の小数点で表示されます
- バイト値（1024未満）は小数点なしの整数として表示されます
- 負の値はNULLを返します
- 8 EB（エクサバイト）までの値をサポートし、BIGINTの全範囲をカバーします

## 例

例1: 様々なバイトサイズをフォーマットします。

```sql
SELECT format_bytes(0);
-- 0 B

SELECT format_bytes(123);
-- 123 B

SELECT format_bytes(1024);
-- 1.00 KB

SELECT format_bytes(4096);
-- 4.00 KB

SELECT format_bytes(123456789);
-- 117.74 MB

SELECT format_bytes(10737418240);
-- 10.00 GB
```

例2: エッジケースとNULL値を処理します。

```sql
SELECT format_bytes(-1);
-- NULL

SELECT format_bytes(NULL);
-- NULL

SELECT format_bytes(0);
-- 0 B
```

例3: テーブルサイズの監視における実用的な使用法。

```sql
-- サイズ情報を持つサンプルテーブルを作成
CREATE TABLE storage_info (
    table_name VARCHAR(64),
    size_bytes BIGINT
);

INSERT INTO storage_info VALUES
('user_profiles', 1073741824),
('transaction_logs', 5368709120),
('product_catalog', 524288000),
('analytics_data', 2199023255552);

-- 人間が読みやすい形式でテーブルサイズを表示
SELECT 
    table_name,
    size_bytes,
    format_bytes(size_bytes) as formatted_size
FROM storage_info
ORDER BY size_bytes DESC;

-- 期待される出力:
-- +------------------+---------------+----------------+
-- | table_name       | size_bytes    | formatted_size |
-- +------------------+---------------+----------------+
-- | analytics_data   | 2199023255552 | 2.00 TB        |
-- | transaction_logs | 5368709120    | 5.00 GB        |
-- | user_profiles    | 1073741824    | 1.00 GB        |
-- | product_catalog  | 524288000     | 500.00 MB      |
-- +------------------+---------------+----------------+
```

例4: 精度と丸めの動作。

```sql
SELECT format_bytes(1536);       -- 1.50 KB (正確に1.5 KB)
SELECT format_bytes(1025);       -- 1.00 KB (2桁に丸め)
SELECT format_bytes(1048576 + 52429);  -- 1.05 MB (2桁に丸め)
```

例5: サポートされる単位の全範囲。

```sql
SELECT format_bytes(512);                    -- 512 B
SELECT format_bytes(2048);                   -- 2.00 KB  
SELECT format_bytes(1572864);                -- 1.50 MB
SELECT format_bytes(2147483648);             -- 2.00 GB
SELECT format_bytes(1099511627776);          -- 1.00 TB
SELECT format_bytes(1125899906842624);       -- 1.00 PB
SELECT format_bytes(1152921504606846976);    -- 1.00 EB
```

## キーワード

FORMAT_BYTES