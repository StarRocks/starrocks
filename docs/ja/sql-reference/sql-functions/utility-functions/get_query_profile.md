---
displayed_sidebar: docs
---

# get_query_profile

`query_id` を使用してクエリのプロファイルを取得します。この関数は、`query_id` が存在しないか間違っている場合は空を返します。

この関数を使用するには、プロファイリング機能を有効にする必要があります。つまり、セッション変数 `enable_profile` を `true` に設定します (`set enable_profile = true;`)。この機能が有効でない場合、空のプロファイルが返されます。

この関数は v3.0 からサポートされています。

## 構文

```Haskell
get_query_profile(x)
```

## パラメータ

`x`: query_id 文字列。サポートされているデータ型は VARCHAR です。

## 戻り値

クエリプロファイルには以下のフィールドが含まれます。クエリプロファイルの詳細については、[Query Profile](../../../administration/query_profile_overview.md) を参照してください。

```SQL
Query:
  Summary:
  Planner:
  Execution Profile 7de16a85-761c-11ed-917d-00163e14d435:
    Fragment 0:
      Pipeline (id=2):
        EXCHANGE_SINK (plan_node_id=18):
        LOCAL_MERGE_SOURCE (plan_node_id=17):
      Pipeline (id=1):
        LOCAL_SORT_SINK (plan_node_id=17):
        AGGREGATE_BLOCKING_SOURCE (plan_node_id=16):
      Pipeline (id=0):
        AGGREGATE_BLOCKING_SINK (plan_node_id=16):
        EXCHANGE_SOURCE (plan_node_id=15):
    Fragment 1:
       ...
    Fragment 2:
       ...
```

## 例

```sql
-- プロファイリング機能を有効にします。
set enable_profile = true;

-- シンプルなクエリを実行します。
select 1;

-- クエリの query_id を取得します。
select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| bd3335ce-8dde-11ee-92e4-3269eb8da7d1 |
+--------------------------------------+

-- クエリプロファイルを取得します。
select get_query_profile('502f3c04-8f5c-11ee-a41f-b22a2c00f66b');

-- regexp_extract 関数を使用して、指定されたパターンに一致するプロファイル内の QueryPeakMemoryUsage を取得します。
select regexp_extract(get_query_profile('bd3335ce-8dde-11ee-92e4-3269eb8da7d1'), 'QueryPeakMemoryUsage: [0-9\.]* [KMGB]*', 0);
+-----------------------------------------------------------------------------------------------------------------------+
| regexp_extract(get_query_profile('bd3335ce-8dde-11ee-92e4-3269eb8da7d1'), 'QueryPeakMemoryUsage: [0-9.]* [KMGB]*', 0) |
+-----------------------------------------------------------------------------------------------------------------------+
| QueryPeakMemoryUsage: 3.828 KB                                                                                        |
+-----------------------------------------------------------------------------------------------------------------------+
```

## 関連関数

- [last_query_id](./last_query_id.md)
- [regexp_extract](../like-predicate-functions/regexp_extract.md)