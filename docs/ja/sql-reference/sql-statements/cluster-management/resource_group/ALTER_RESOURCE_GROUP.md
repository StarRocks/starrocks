---
displayed_sidebar: docs
---

# ALTER RESOURCE GROUP

## 説明

リソースグループの設定を変更します。

:::tip

この操作には、対象のリソースグループに対する ALTER 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

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

| **パラメータ**      | **説明**                                                     |
| ------------------- | ------------------------------------------------------------ |
| resource_group_name | 変更するリソースグループの名前。                             |
| ADD                 | リソースグループにクラシファイア（分類器）を追加します。クラシファイアの定義方法については、[CREATE RESOURCE GROUP - Parameters](CREATE_RESOURCE_GROUP.md) を参照してください。 |
| DROP                | クラシファイア ID を使用してリソースグループからクラシファイアを削除します。クラシファイアの ID は [SHOW RESOURCE GROUP](SHOW_RESOURCE_GROUP.md) ステートメントで確認できます。 |
| DROP ALL            | リソースグループからすべてのクラシファイアを削除します。     |
| WITH                | リソースグループのリソース制限を変更します。リソース制限の設定方法については、[CREATE RESOURCE GROUP - Parameters](CREATE_RESOURCE_GROUP.md) を参照してください。 |

## 例

例 1: 新しいクラシファイアをリソースグループ `rg1` に追加します。

```SQL
ALTER RESOURCE GROUP rg1 ADD (user='root', query_type in ('select'));
```

例 2: ID `300040`、`300041`、`300041` のクラシファイアをリソースグループ `rg1` から削除します。

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
    'cpu_weight' = '10'
);
```