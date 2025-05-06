---
displayed_sidebar: docs
---

# be_bvars

`be_bvars` は bRPC に関する統計情報を提供します。StarRocks のいくつかのコンポーネントに対する RPC レイテンシー、QPS、その他の統計を確認できます。

`be_bvars` で提供されるフィールドは次のとおりです。

| **Field** | **Description**                                              |
| --------- | ------------------------------------------------------------ |
| BE_ID     | bvar が存在する BE の ID。                                   |
| NAME      | bvar の名前。                                                |
| DESC      | 重要な統計情報を含む bvar の説明。                           |