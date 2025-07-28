---
displayed_sidebar: docs
---

# applicable_roles

`applicable_roles` は、現在のユーザーに適用可能なロールに関する情報を提供します。

`applicable_roles` には以下のフィールドが提供されています:

| **フィールド**       | **説明**                                         |
| -------------- | ------------------------------------------------ |
| USER           | ロールが適用されるユーザー。                     |
| HOST           | ユーザーが接続するホスト。                       |
| GRANTEE        | ロールを付与したユーザーまたはロール。           |
| GRANTEE_HOST   | 付与者が接続するホスト。                         |
| ROLE_NAME      | ロールの名前。                                   |
| ROLE_HOST      | ロールに関連付けられたホスト。                   |
| IS_GRANTABLE   | ロールを他のユーザー��付与できるかどうか（`YES` または `NO`）。 |
| IS_DEFAULT     | ロールがデフォルトロールであるかどうか（`YES` または `NO`）。 |
| IS_MANDATORY   | ロールが必須であるかどうか（`YES` または `NO`）。 |
