---
displayed_sidebar: docs
---

# ALTER RESOURCE GROUP

## 説明

リソースグループの設定を変更します。

## 構文

```SQL
ALTER RESOURCE GROUP resource_group_name
{  ADD CLASSIFIER1, CLASSIFIER2, ...
 | DROP (CLASSIFIER_ID_1, CLASSIFIER_ID_2, ...)
 | DROP ALL
 | WITH resource_limit 
};
```

## パラメータ

| **パラメータ**      | **説明**                                                      |
| ------------------- | ------------------------------------------------------------ |
| resource_group_name | 変更するリソースグループの名前。                             |
| ADD                 | リソースグループにクラシファイア（分類器）を追加します。[CREATE RESOURCE GROUP - Parameters](../Administration/CREATE_RESOURCE_GROUP.md) を参照して、クラシファイアの定義方法について詳しく学んでください。 |
| DROP                | クラシファイア ID を使用してリソースグループからクラシファイアを削除します。[SHOW RESOURCE GROUP](../Administration/SHOW_RESOURCE_GROUP.md) ステートメントを使用して、クラシファイアの ID を確認できます。 |
| DROP ALL            | リソースグループからすべてのクラシファイアを削除します。     |
| WITH                | リソースグループのリソース制限を変更します。[CREATE RESOURCE GROUP - Parameters](../Administration/CREATE_RESOURCE_GROUP.md) を参照して、リソース制限の設定方法について詳しく学んでください。 |

## 例

例 1: リソースグループ `rg1` に新しいクラシファイアを追加します。

```SQL
ALTER RESOURCE GROUP rg1 ADD (user='root', query_type in ('select'));
```

例 2: リソースグループ `rg1` から ID `300040`、`300041`、`300041` のクラシファイアを削除します。

```SQL
ALTER RESOURCE GROUP rg1 DROP (300040, 300041, 300041);
```

例 3: リソースグループ `rg1` からすべてのクラシファイアを削除します。

```SQL
ALTER RESOURCE GROUP rg1 DROP ALL;
```

例 4: リソースグループ `rg1` のリソース制限を変更します。

```SQL
ALTER RESOURCE GROUP rg1 WITH (
    'cpu_core_limit' = '20'
);
```