---
displayed_sidebar: docs
---

# SELECT

SELECT は、1 つ以上のテーブル、ビュー、またはマテリアライズドビューからデータをクエリします。SELECT は、独立したステートメントまたは他のステートメントにネストされた句として機能します。SELECT 句の出力は、他のステートメントの入力として使用できます。

StarRocks のクエリステートメントは、基本的に SQL92 標準に準拠しています。

:::note
StarRocks 内部テーブルのテーブル、ビュー、またはマテリアライズドビューからデータをクエリするには、これらのオブジェクトに対する SELECT 権限が必要です。外部データソースのテーブル、ビュー、またはマテリアライズドビューからデータをクエリするには、対応する external catalog に対する USAGE 権限が必要です。
:::

## 構文

```SQL
SELECT select_list FROM table_reference [, ...]
```

SELECT ステートメントは、通常、次の句で構成されます。

- [Common Table Expression (CTE)](./SELECT_CTE.md)
- [Join](./SELECT_JOIN.md)
- [ORDER BY](./SELECT_ORDER_BY.md)
- [GROUP BY](./SELECT_GROUP_BY.md)
- [HAVING](./SELECT_HAVING.md)
- [LIMIT](./SELECT_LIMIT.md)
- [OFFSET](./SELECT_OFFSET.md)
- [UNION](./SELECT_UNION.md)
- [INTERSECT](./SELECT_INTERSECT.md)
- [EXCEPT/MINUS](./SELECT_EXCEPT_MINUS.md)
- [DISTINCT](./SELECT_DISTINCT.md)
- [サブクエリ](./SELECT_subquery.md)
- [WHERE と演算子](./SELECT_WHERE_operator.md)
- [エイリアス](./SELECT_alias.md)
- [PIVOT](./SELECT_PIVOT.md)
- [EXCLUDE](./SELECT_EXCLUDE.md)

詳しい手順については、該当する項目を参照してください。
