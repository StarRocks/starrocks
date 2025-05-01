---
displayed_sidebar: docs
---

# CREATE FUNCTION

## Description

ユーザー定義関数 (UDF) を作成します。現在、Java UDF のみ作成可能で、スカラー関数、ユーザー定義集計関数 (UDAF)、ユーザー定義ウィンドウ関数 (UDWF)、ユーザー定義テーブル関数 (UDTF) が含まれます。

**Java UDF のコンパイル、作成、使用方法の詳細については、 [Java UDF](../../sql-functions/JAVA_UDF.md) を参照してください。**

> **NOTE**
>
> グローバル UDF を作成するには、SYSTEM レベルの CREATE GLOBAL FUNCTION 権限が必要です。データベース全体の UDF を作成するには、DATABASE レベルの CREATE FUNCTION 権限が必要です。

## Syntax

```sql
CREATE [GLOBAL][AGGREGATE | TABLE] FUNCTION function_name
(arg_type [, ...])
RETURNS return_type
PROPERTIES ("key" = "value" [, ...])
```

## Parameters

| **Parameter**      | **Required** | **Description**                                                     |
| ------------- | -------- | ------------------------------------------------------------ |
| GLOBAL        | No       | グローバル UDF を作成するかどうか、v3.0 からサポートされています。  |
| AGGREGATE     | No       | UDAF または UDWF を作成するかどうか。       |
| TABLE         | No       | UDTF を作成するかどうか。`AGGREGATE` と `TABLE` の両方が指定されていない場合、スカラー関数が作成されます。               |
| function_name | Yes       | 作成したい関数の名前。このパラメータにデータベース名を含めることができます。例えば、`db1.my_func`。`function_name` にデータベース名が含まれている場合、UDF はそのデータベースに作成されます。そうでない場合、UDF は現在のデータベースに作成されます。新しい関数の名前とそのパラメータは、宛先データベース内の既存の名前と同じにすることはできません。そうでない場合、関数は作成できません。関数名が同じでもパラメータが異なる場合は作成が成功します。 |
| arg_type      | Yes       | 関数の引数の型。追加された引数は `, ...` で表現できます。サポートされているデータ型については、 [Java UDF](../../sql-functions/JAVA_UDF.md#mapping-between-sql-data-types-and-java-data-types) を参照してください。|
| return_type      | Yes       | 関数の戻り値の型。サポートされているデータ型については、 [Java UDF](../../sql-functions/JAVA_UDF.md#mapping-between-sql-data-types-and-java-data-types) を参照してください。 |
| PROPERTIES    | Yes       | 関数のプロパティは、作成する UDF のタイプによって異なります。詳細については、 [Java UDF](../../sql-functions/JAVA_UDF.md#step-6-create-the-udf-in-starrocks) を参照してください。 |