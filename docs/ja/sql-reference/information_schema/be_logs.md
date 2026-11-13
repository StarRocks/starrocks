---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "be_logsは各BEノードのログに関する情報を提供します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# be_logs

`be_logs` は各 BE ノードのログに関する情報を提供します。

`be_logs` には以下のフィールドが提供されています:

| **フィールド**    | **説明**                                         |
| ----------- | ------------------------------------------------ |
| BE_ID       | BE ノードの ID。                                   |
| LEVEL       | ログレベル（例: `INFO`、`WARNING`、`ERROR`）。      |
| TIMESTAMP   | ログエントリのタイムスタンプ。                   |
| TID         | ログエントリを生成したスレッド ID。              |
| LOG         | ログメッセージの内容。                           |