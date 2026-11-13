---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "be_threadsは各BEノードで実行されているスレッドに関する情報を提供します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# be_threads

`be_threads` は各 BE ノードで実行されているスレッドに関する情報を提供します。

`be_threads` には以下のフィールドが提供されています:

| **フィールド**       | **説明**                                         |
| -------------- | ------------------------------------------------ |
| BE_ID          | BE ノードの ID。                                   |
| GROUP          | スレッドグループ名。                             |
| NAME           | スレッド名。                                     |
| PTHREAD_ID     | Pthread ID。                                     |
| TID            | スレッド ID。                                    |
| IDLE           | スレッドがアイドル状態であるかどうか（`true`）またはそうでないか（`false`）を示します。 |
| FINISHED_TASKS | スレッドによって完了したタスクの数。             |
| BOUND_CPUS     | スレッドがバインドされている CPU。               |
