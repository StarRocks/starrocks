---
displayed_sidebar: docs
---

# approx_top_k

`expr` 内で最も頻繁に出現する上位 `k` 個のアイテム値とその概算カウントを返します。

この関数は v3.0 からサポートされています。

## Syntax

```Haskell
APPROX_TOP_K(<expr> [ , <k> [ , <counter_num> ] ] )
```

## Arguments

- `expr`: STRING、BOOLEAN、DATE、DATETIME、または数値型の式。
- `k`: オプションの INTEGER リテラルで、0 より大きい値。`k` が指定されていない場合、デフォルトは `5` です。最大値は `100000` です。
- `counter_num`: オプションの INTEGER リテラルで、`k` 以上の値。`counter_num` が大きいほど、結果はより正確になりますが、CPU とメモリのコストも増加します。

  - 最大値は `100000` です。
  - `counter_num` が指定されていない場合、デフォルトは `max(min(2 * k, 100), 100000)` です。

## Returns

結果は STRUCT 型の ARRAY として返され、各 STRUCT には値のための `item` フィールド（元の入力型）と、概算出現回数を持つ `count` フィールド（BIGINT 型）が含まれます。配列は `count` の降順でソートされます。

この集計関数は、式 `expr` 内で最も頻繁に出現する上位 `k` 個のアイテム値とその概算カウントを返します。各カウントの誤差は最大で `2.0 * numRows / counter_num` になる可能性があります。ここで `numRows` は行の総数です。`counter_num` の値が高いほど、メモリ使用量が増加する代わりに精度が向上します。`counter_num` 未満の異なるアイテムを持つ式は、正確なアイテムカウントを生成します。結果には `NULL` 値も独自のアイテムとして含まれます。

## Examples

[scores](../Window_function.md#window-function-sample-table) テーブルのデータを例として使用します。

```plaintext
-- 各科目のスコア分布を計算します。
MySQL > SELECT subject, APPROX_TOP_K(score)  AS top_k FROM scores GROUP BY subject;
+---------+--------------------------------------------------------------------------------------------------------------------+
| subject | top_k                                                                                                              |
+---------+--------------------------------------------------------------------------------------------------------------------+
| physics | [{"item":99,"count":2},{"item":null,"count":1},{"item":100,"count":1},{"item":85,"count":1},{"item":60,"count":1}] |
| english | [{"item":null,"count":1},{"item":92,"count":1},{"item":98,"count":1},{"item":100,"count":1},{"item":85,"count":1}] |
| NULL    | [{"item":90,"count":1}]                                                                                            |
| math    | [{"item":80,"count":2},{"item":null,"count":1},{"item":92,"count":1},{"item":95,"count":1},{"item":70,"count":1}]  |
+---------+--------------------------------------------------------------------------------------------------------------------+

-- 数学科目のスコア分布を計算します。
MySQL > SELECT subject, APPROX_TOP_K(score)  AS top_k FROM scores WHERE subject IN  ('math') GROUP BY subject;
+---------+-------------------------------------------------------------------------------------------------------------------+
| subject | top_k                                                                                                             |
+---------+-------------------------------------------------------------------------------------------------------------------+
| math    | [{"item":80,"count":2},{"item":null,"count":1},{"item":95,"count":1},{"item":92,"count":1},{"item":70,"count":1}] |
+---------+-------------------------------------------------------------------------------------------------------------------+
```

## keyword

APPROX_TOP_K