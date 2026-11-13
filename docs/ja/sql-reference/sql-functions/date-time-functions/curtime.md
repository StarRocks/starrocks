---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "現在のタイムゾーンを取得し、VARCHAR 値を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# curtime,current_time

現在の時刻を取得し、TIME 型の値を返します。

この関数は、異なるタイムゾーンで異なる結果を返すことがあります。詳細については、[タイムゾーンの設定](../../../administration/management/timezone.md)を参照してください。

## 構文

```plaintext
TIME CURTIME()
```

## 例

```Plain Text
MySQL > select current_time();
+----------------+
| current_time() |
+----------------+
| 15:25:47       |
+----------------+
```

## キーワード

CURTIME,CURRENT_TIME