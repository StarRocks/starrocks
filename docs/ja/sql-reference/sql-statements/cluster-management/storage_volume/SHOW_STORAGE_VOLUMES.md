---
displayed_sidebar: docs
---

# SHOW STORAGE VOLUMES

## 説明

StarRocks クラスター内のストレージボリュームを表示します。この機能は v3.1 からサポートされています。

:::tip

この操作には特別な権限は必要ありません。

:::

## 構文

```SQL
SHOW STORAGE VOLUMES [ LIKE '<pattern>' ]
```

## パラメーター

| **パラメーター** | **説明**                                 |
| --------------- | ---------------------------------------- |
| pattern         | ストレージボリュームを一致させるために使用されるパターン。 |

## 戻り値

| **戻り値**      | **説明**                     |
| --------------- | ---------------------------- |
| Storage Volume  | ストレージボリュームの名前。 |

## 例

例 1: StarRocks クラスター内のすべてのストレージボリュームを表示します。

```Plain
MySQL > SHOW STORAGE VOLUMES;
+----------------+
| Storage Volume |
+----------------+
| my_s3_volume   |
+----------------+
1 row in set (0.01 sec)
```

## 関連する SQL ステートメント

- [CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](DESC_STORAGE_VOLUME.md)