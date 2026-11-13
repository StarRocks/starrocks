---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Information schema placeholder view not implemented in StarRocks."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# user_privileges

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`user_privileges` は、ユーザー権限に関する情報を提供します。

`user_privileges` には以下のフィールドが含まれています。

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 権限が付与されているユーザーの名前。                        |
| TABLE_CATALOG  | catalog の名前。この値は常に `def` です。                    |
| PRIVILEGE_TYPE | 付与された権限。グローバルレベルで付与可能な任意の権限の値を持つことができます。 |
| IS_GRANTABLE   | ユーザーが `GRANT OPTION` 権限を持っている場合は `YES`、それ以外の場合は `NO`。出力には `PRIVILEGE_TYPE='GRANT OPTION'` として `GRANT OPTION` を別行でリストしません。 |