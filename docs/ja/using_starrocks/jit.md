---
displayed_sidebar: docs
sidebar_position: 130
---

# JIT Compilation for Expressions

このトピックでは、StarRocks における式の JIT コンパイルの有効化と設定方法について説明します。

## Overview

ジャストインタイムコンパイル (JIT) は、実行時に機械コードを生成して実行することです。インタープリタと比較して、JIT コンパイラは最も使用されるコード部分の実行効率を大幅に向上させることができます。StarRocks は特定の複雑な式に対して JIT コンパイルをサポートしており、パフォーマンスを大幅に向上させることができます。

## Usage

バージョン 3.3.0 以降、StarRocks はメモリ制限（BE 設定項目 `mem_limit` で設定） が 16 GB 以上の BE ノードに対してデフォルトで JIT コンパイルを有効にしています。JIT コンパイルは一定のメモリリソースを消費するため、16 GB 未満のメモリを持つ BE ノードではデフォルトで無効になっています。

次のパラメータを使用して、式の JIT コンパイルを有効化および設定できます。

### jit_lru_cache_size (BE configuration)

- Default: 0
- Type: Int
- Unit: GB
- Is mutable: Yes
- Description: JIT コンパイルのための LRU キャッシュサイズ。0 より大きい値に設定すると、キャッシュの実際のサイズを表します。0 以下に設定すると、システムは `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` の式を使用してキャッシュを適応的に設定します（ノードの `mem_limit` は 16 GB 以上でなければなりません）。
- Introduced in: -

### jit_level (System variable)

- Description: 式の JIT コンパイルが有効になるレベル。 有効な値:
  - `1`: システムはコンパイル可能な式に対して JIT コンパイルを適応的に有効にします。
  - `-1`: すべてのコンパイル可能な非定数式に対して JIT コンパイルが有効になります。
  - `0`: JIT コンパイルは無効です。この機能にエラーが返された場合、手動で無効にすることができます。
- Default: 1
- Data type: Int
- Introduced in: -

## Feature support

### Supported expressions

- `+`, `-`, `*`, `/`, `%`, `&`, `|`, `^`, `>>`, `<<`
- CAST を使用したデータ型変換
- CASE WHEN
- `=`, `!=`, `>`, `>=`, `<`, `<=`, `<=>`
- `AND`, `OR`, `NOT`

### Supported operators

- フィルター用 OLAP Scan Operator
- Projection Operator
- 式用 Aggregate Operator
- HAVING
- 式用 Sort Operator

### Supported data types

- BOOLEAN
- TINYINT
- SMALLINT
- INT
- BIGINT
- LARGEINT
- FLOAT
- DOUBLE