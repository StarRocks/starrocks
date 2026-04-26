---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# ベンチマーク catalog

ベンチマーク catalog は、標準的なベンチマークスイートのためにその場でデータを生成する、組み込みの external catalog です。これにより、データをロードせずに TPC-H、TPC-DS、および SSB スキーマに対してクエリを実行できます。

> **NOTE**
>
> すべてのデータはクエリ時にその場で生成され、永続化されません。この catalog をベンチマークソースとして使用しないでください。

## ベンチマーク catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### パラメータ

#### `catalog_name`

ベンチマーク catalog の名前。命名規則は次のとおりです。

- 名前には、文字、数字 (0-9)、およびアンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字が区別され、長さが 1023 文字を超えることはできません。

#### `comment`

ベンチマーク catalog の説明。このパラメータはオプションです。

#### `PROPERTIES`

ベンチマーク catalog のプロパティ。`PROPERTIES` には、次のパラメータを含める必要があります。

| Parameter | Required | Default value | Description |
| --------- | -------- | ------------- | ----------- |
| type      | Yes      | None          | データソースのタイプ。値を `benchmark` に設定します。 |
| scale     | No       | `1`           | 生成されたデータのスケールファクタ。値は 0 より大きくなければなりません。整数以外の値もサポートされています。 |

### 例

```SQL
CREATE EXTERNAL CATALOG bench
PROPERTIES ("type" = "benchmark", "scale" = "1.0");
```

## ベンチマークデータのクエリ

ベンチマーク catalog は、次のデータベースを公開します。

- `tpcds`: TPC-DS スキーマ。
- `tpch`: TPC-H スキーマ。
- `ssb`: Star Schema Benchmark スキーマ。

次の例は、catalog に切り替えて SSB スキーマをクエリする方法を示しています。

```SQL
SET CATALOG bench;
SHOW DATABASES;
USE ssb;
SHOW TABLES;
SELECT COUNT(*) FROM date;
```

## 使用上の注意

- スキーマとテーブルは組み込みのベンチマーク定義に固定されているため、この catalog 内のオブジェクトを作成、変更、または削除することはできません。
- データはクエリ時にその場で生成され、外部システムには保存されません。