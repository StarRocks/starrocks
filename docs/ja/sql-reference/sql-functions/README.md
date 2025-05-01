---
displayed_sidebar: docs
sidebar_position: 0.5
---

# Functions

import DocCardList from '@theme/DocCardList';

StarRocks は、データクエリと分析を容易にする豊富な関数セットを提供します。一般的に使用される関数に加えて、StarRocks は ARRAY、JSON、MAP、STRUCT 関数などの半構造化関数をサポートしています。また、高階 [Lambda 関数](Lambda_expression.md) もサポートしています。これらの関数がビジネス要件を満たさない場合は、[Java UDF](JAVA_UDF.md) を使用して関数をコンパイルできます。StarRocks は [Hive Bitmap UDFs](hive_bitmap_udf.md) も提供しています。Hive で Bitmap データを生成し、その後 Bitmap を StarRocks にロードできます。また、StarRocks で生成された Bitmap データを Hive にエクスポートして、他のシステムで使用することもできます。

<DocCardList />