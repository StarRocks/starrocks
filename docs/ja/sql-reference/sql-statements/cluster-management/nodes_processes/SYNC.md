---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SYNC は、異なるセッション間でデータの一貫性を同期します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SYNC

異なるセッション間でデータの一貫性を同期します。

現在、StarRocks は同じセッション内でのみ一貫性を保証できます。つまり、データ書き込み操作を開始したセッションだけが、操作成功直後に最新のデータをすぐに読み取ることができます。他のセッションから最新のデータをすぐに読み取りたい場合は、SYNC ステートメントを使用してデータの一貫性を同期する必要があります。このステートメントを実行しない場合、セッション間でミリ秒レベルの遅延が発生することがあります。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```SQL
SYNC
```

## 例

```Plain
mysql> SYNC;
Query OK, 0 rows affected (0.00 sec)
```