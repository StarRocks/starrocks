---
displayed_sidebar: docs
---

# concat_ws

## 説明

この関数は、最初の引数 sep を区切り文字として使用し、2番目の引数とそれ以降を結合して文字列を形成します。区切り文字が NULL の場合、結果は NULL になります。concat_ws は空の文字列をスキップしませんが、NULL 値はスキップします。

## 構文

```Haskell
VARCHAR concat_ws(VARCHAR sep, VARCHAR str,...)
```

## 例

```Plain Text
MySQL > select concat_ws("Rock", "Star", "s");
+--------------------------------+
| concat_ws('Rock', 'Star', 's') |
+--------------------------------+
| StarRocks                      |
+--------------------------------+

MySQL > select concat_ws(NULL, "Star", "s");
+------------------------------+
| concat_ws(NULL, 'Star', 's') |
+------------------------------+
| NULL                         |
+------------------------------+
1 row in set (0.01 sec)

MySQL > StarRocks > select concat_ws("Rock", "Star", NULL, "s");
+--------------------------------------+
| concat_ws('Rock', 'Star', NULL, 's') |
+--------------------------------------+
| StarRocks                            |
+--------------------------------------+
1 row in set (0.04 sec)
```

## キーワード

CONCAT_WS, CONCAT, WS