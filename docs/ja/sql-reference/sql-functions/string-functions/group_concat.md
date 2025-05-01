---
displayed_sidebar: docs
---

# group_concat

## 説明

`sep` 引数を使用して、グループ内の非 NULL 値を単一の文字列に連結します。指定しない場合、デフォルトで `,` が使用されます。この関数は、複数の行の列の値を1つの文字列に連結するために使用できます。

group_concat は、3.0.6以降の3.0バージョンおよび3.1.3以降の3.1バージョンで DISTINCT と ORDER BY をサポートしています。

## 構文

```SQL
VARCHAR GROUP_CONCAT([DISTINCT] expr [,expr ...]
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]]
             [SEPARATOR sep])
```

## パラメータ

- `expr`: 連結する値で、NULL 値は無視されます。VARCHAR に評価される必要があります。出力文字列から重複する値を排除するには、`DISTINCT` を指定することができます。複数の `expr` を直接連結したい場合は、[concat](./concat.md) または [concat_ws](./concat_ws.md) を使用してフォーマットを指定してください。
- ORDER BY の項目は、符号なし整数（1から始まる）、列名、または通常の式を使用できます。結果はデフォルトで昇順にソートされます。ASC キーワードを明示的に指定することもできます。降順にソートしたい場合は、ソートする列名に DESC キーワードを追加します。
- `sep`: 異なる行の非 NULL 値を連結するために使用されるオプションのセパレータです。指定しない場合、デフォルトで `,`（カンマ）が使用されます。セパレータを排除するには、空の文字列 `''` を指定します。

> **注意**
>
> v3.0.6 および v3.1.3 以降では、セパレータを指定する際の動作が変更されています。セパレータを宣言するには `SEPARATOR` を使用する必要があります。例えば、`select group_concat(name SEPARATOR '-') as res from ss;` のようにします。

## 戻り値

各グループに対して文字列値を返し、非 NULL 値がない場合は NULL を返します。

返される文字列の長さを制限するには、[セッション変数](../../System_variable.md) `group_concat_max_len` を設定します。デフォルトは 1024 です。最小値: 4。単位: 文字。

例:

```sql
SET [GLOBAL | SESSION] group_concat_max_len = <value>;
```

## 例

1. 科目のスコアを含むテーブル `ss` を作成します。

   ```sql
   CREATE TABLE `ss` (
     `id` int(11) NULL COMMENT "",
     `name` varchar(255) NULL COMMENT "",
     `subject` varchar(255) NULL COMMENT "",
     `score` int(11) NULL COMMENT ""
   ) ENGINE=OLAP
   DUPLICATE KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 4
   PROPERTIES (
   "replication_num" = "1"
   );

   insert into ss values (1,"Tom","English",90);
   insert into ss values (1,"Tom","Math",80);
   insert into ss values (2,"Tom","English",NULL);
   insert into ss values (2,"Tom",NULL,NULL);
   insert into ss values (3,"May",NULL,NULL);
   insert into ss values (3,"Ti","English",98);
   insert into ss values (4,NULL,NULL,NULL);
   insert into ss values (NULL,"Ti","Phy",98);

   select * from ss order by id;
   +------+------+---------+-------+
   | id   | name | subject | score |
   +------+------+---------+-------+
   | NULL | Ti   | Phy     |    98 |
   |    1 | Tom  | English |    90 |
   |    1 | Tom  | Math    |    80 |
   |    2 | Tom  | English |  NULL |
   |    2 | Tom  | NULL    |  NULL |
   |    3 | May  | NULL    |  NULL |
   |    3 | Ti   | English |    98 |
   |    4 | NULL | NULL    |  NULL |
   +------+------+---------+-------+
   ```

2. group_concat を使用します。

  例 1: デフォルトのセパレータで名前を文字列に連結し、NULL 値は無視されます。重複する名前は保持されます。

  ```sql
   select group_concat(name) as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Tom,Tom,Ti,Tom,Tom,May,Ti |
   +---------------------------+
  ```

  例 2: セパレータ `-` で名前を文字列に連結し、NULL 値は無視されます。重複する名前は保持されます。

  ```sql
   select group_concat(name SEPARATOR '-') as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Ti-May-Ti-Tom-Tom-Tom-Tom |
   +---------------------------+
  ```

  例 3: デフォルトのセパレータで重複しない名前を文字列に連結し、NULL 値は無視されます。重複する名前は削除されます。

  ```sql
   select group_concat(distinct name) as res from ss;
   +---------------------------+
   | res                       |
   +---------------------------+
   | Ti,May,Tom                |
   +---------------------------+
  ```

  例 4: 同じ ID の名前と科目の文字列を `score` の昇順で連結します。例えば、`TomMath` と `TomEnglish` は ID 1 を共有しており、`score` の昇順でカンマで連結されます。

  ```sql
   select id, group_concat(distinct name,subject order by score) as res from ss group by id order by id;
   +------+--------------------+
   | id   | res                |
   +------+--------------------+
   | NULL | TiPhy              |
   |    1 | TomMath,TomEnglish |
   |    2 | TomEnglish         |
   |    3 | TiEnglish          |
   |    4 | NULL               |
   +------+--------------------+
   ```

  例 5: group_concat は concat() とネストされており、`name`、`-`、`subject` を文字列として結合します。同じ行の文字列は `score` の昇順でソートされます。

  ```sql
   select id, group_concat(distinct concat(name, '-',subject) order by score) as res from ss group by id order by id;
   +------+----------------------+
   | id   | res                  |
   +------+----------------------+
   | NULL | Ti-Phy               |
   |    1 | Tom-Math,Tom-English |
   |    2 | Tom-English          |
   |    3 | Ti-English           |
   |    4 | NULL                 |
   +------+----------------------+
   ```

  例 6: 一致する結果が見つからず、NULL が返されます。

  ```sql
  select group_concat(distinct name) as res from ss where id < 0;
   +------+
   | res  |
   +------+
   | NULL |
   +------+
   ```

  例 7: 返される文字列の長さを6文字に制限します。

  ```sql
   set group_concat_max_len = 6;

   select id, group_concat(distinct name,subject order by score) as res from ss group by id order by id;
   +------+--------+
   | id   | res    |
   +------+--------+
   | NULL | TiPhy  |
   |    1 | TomMat |
   |    2 | NULL   |
   |    3 | TiEngl |
   |    4 | NULL   |
   +------+--------+
   ```

## キーワード

GROUP_CONCAT,CONCAT,ARRAY_AGG