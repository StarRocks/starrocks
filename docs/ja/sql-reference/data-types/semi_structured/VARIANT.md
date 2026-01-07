---
displayed_sidebar: docs
---

# VARIANT

バージョン 4.0 以降、StarRocks は VARIANT データ型をサポートしています。VARIANT は複数のデータ型の値を格納できる半構造化型で、Parquet の variant エンコーディングを用いたデータの読み取りや書き出しに利用できます。

Parquet の variant エンコーディングについては、[Parquet Variant Encoding specification](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) を参照してください。

## VARIANT の使用

### Parquet からの読み取り

StarRocks は、Parquet の variant エンコーディングを使用する Iceberg テーブルから VARIANT 列を読み取ることができます。

### Parquet への書き込み

StarRocks のファイル書き込み機能により、VARIANT 列を Parquet に書き出すことができます（未分割の variant エンコーディング）。

## 制限と注意事項

- VARIANT 値のサイズ上限は 16 MB です。
- 現在は未分割（unshredded）の variant 値のみを読み書きできます。
- ネスト構造の最大深度は、基となる Parquet ファイル構造に依存します。
