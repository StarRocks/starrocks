---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "enginesはStarRocksで実装されていないプレースホルダービューです。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# engines

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`engines` はストレージエンジンに関する情報を提供します。

`engines` には以下のフィールドが提供されます:

| **Field**    | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| ENGINE       | ストレージエンジンの名前。                                   |
| SUPPORT      | ストレージエンジンに対するサーバーのサポートレベル。 有効な値:<ul><li>`YES`: エンジンはサポートされており、アクティブです。</li><li>`DEFAULT`: `YES` と同様ですが、これがデフォルトのエンジンです。</li><li>`NO`: エンジンはサポートされていません。</li><li>`DISABLED`: エンジンはサポートされていますが、無効化されています。</li></ul> |
| COMMENT      | ストレージエンジンの簡単な説明。                             |
| TRANSACTIONS | ストレージエンジンがトランザクションをサポートしているかどうか。 |
| XA           | ストレージエンジンが XA トランザクションをサポートしているかどうか。 |
| SAVEPOINTS   | ストレージエンジンがセーブポイントをサポートしているかどうか。 |