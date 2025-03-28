---
displayed_sidebar: docs
---

# ADMIN SET PARTITION VERSION

## 説明

パーティションを特定のデータバージョンに設定します。

パーティションバージョンを手動で設定することは高リスクな操作であり、クラスターメタデータに問題が発生した場合にのみ推奨されます。通常、パーティションのバージョンはその中の tablets のバージョンと一致しています。

:::tip

この操作には SYSTEM レベルの OPERATE 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
ADMIN SET TABLE <table_name> PARTITION ( <partition_name> | <partition_id> ) 
VERSION TO <version>
```

## パラメータ

- `table_name`: パーティションが属するテーブルの名前。
- `partition_name`: パーティションの名前。`partition_name` または `partition_id` のいずれかを指定する必要があります。非パーティションテーブルの場合、`partition_name` はテーブル名と同じです。
- `partition_id`: パーティションの ID。`partition_name` または `partition_id` のいずれかを指定する必要があります。ランダムバケット法を使用するテーブルでは、`partition_id` のみでパーティションを指定できます。
- `version`: パーティションに設定したいバージョン。

## 例

1. 非パーティションテーブル `t1` のバージョンを `10` に設定します。

    ```sql
    ADMIN SET TABLE t1 PARTITION(t1) VERSION TO 10;
    ```

2. テーブル `t2` のパーティション `p1` のバージョンを `10` に設定します。

    ```sql
    ADMIN SET TABLE t2 PARTITION(p1) VERSION TO 10;
    ```

3. ID が `123456` のパーティションのバージョンを `10` に設定します。`t3` はランダムバケット法を使用するテーブルです。

    ```sql
    ADMIN SET TABLE t3 PARTITION('123456') VERSION TO 10;
    ```