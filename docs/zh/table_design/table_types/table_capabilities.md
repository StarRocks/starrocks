---
displayed_sidebar: docs
---

# 各类表的能力对比

## Key 列和排序键

<table>
<thead>
<tr><th> </th><th><strong>主键表 (Primary Key table)</strong></th><th><strong>明细表 (Duplicate Key table)</strong></th><th><strong>聚合表 (Aggregate table)</strong></th><th><strong>更新表 (Unique Key table)</strong></th></tr>
</thead>
<tr><td width= "200"><strong>Key 列和唯一约束</strong></td><td>主键 `PRIMARY KEY` 具有唯一约束和非空约束。</td><td>`DUPLICATE KEY` 不具有唯一约束。</td><td>聚合键 `AGGREGATE KEY` 具有唯一约束。</td><td>唯一键 `UNIQUE KEY` 具有唯一约束。</td></tr><tr><td><strong>Key 列和数据变更的关系（逻辑关系）</strong></td><td>如果新数据的主键值与表中原数据的主键值相同，则存在唯一约束冲突，此时新数据会替代原数据。<br />与更新表相比，主键表增强了其底层存储引擎，已经可以取代更新表。</td><td>`Duplicate Key` 不具有唯一约束，因此如果新数据的 Duplicate Key 与表中原数据相同，则新旧数据都会存在表中。</td><td>如果新数据与表中原数据存在唯一约束冲突，则会根据聚合键和 Value 列的聚合函数聚合新旧数据。</td><td>如果新数据与表中原数据存在唯一约束冲突，则新数据会替代原数据。<br />更新表实际可以视为聚合函数为 replace 的聚合表。</td></tr><tr><td><strong>Key 列和排序键的关系</strong></td><td>自 3.0.0 起，主键表中两者解耦。主键表支持使用 `ORDER BY` 指定排序键和使用 `PRIMARY KEY` 指定主键。</td><td>自 3.3.0 起，明细表支持使用 `ORDER BY` 指定排序键，如果同时使用 `ORDER BY` 和 `DUPLICATE KEY`，则 `DUPLICATE KEY` 无效。</td><td>自 3.3.0 起，聚合表中两者解耦。聚合表支持使用 `ORDER BY` 指定排序键和使用 `AGGREGATE KEY` 指定聚合键。排序键和聚合键中的列需要保持一致，但是列的顺序不需要保持一致。</td><td>自 3.3.0 起，更新表中两者解耦。更新表支持使用 `ORDER BY` 指定排序键和使用 `UNIQUE KEY` 指定唯一键。排序键和唯一键中的列需要保持一致，但是列的顺序不需要保持一致。</td></tr><tr><td><strong>Key 列和排序键支持的数据类型</strong></td><td>数值（包括整型、布尔）、字符串、时间日期。</td><td colspan="3">数值（包括整型、布尔、Decimal）、字符串、时间日期。</td></tr><tr><td><strong>Key 和分区/分桶列的关系</strong></td><td>分区列、分桶列必须在主键中。</td><td>无</td><td>分区列、分桶列必须在聚合键中。</td><td>分区列、分桶列必须在唯一键中。</td></tr>
</table>

## Key 列和 Value 列的数据类型

表中 Key 列支持数据类型为数值（包括整型、布尔和 DECIMAL）、字符串、时间日期。

:::note

主键表的 Key 列不支持为 DECIMAL。

:::

而表中 Value 列支持基础的数据类型，包括数值、字符串、时间日期。不同类型的表中 Value 列对于 BITMAP、HLL 以及半结构化类型的支持度不同，具体如下：

<table>
<thead>
<tr><th> </th><th><strong>主键表 (Primary Key table)</strong></th><th><strong>明细表 (Duplicate Key table)</strong></th><th><strong>聚合表 (Aggregate table)</strong></th><th><strong>更新表 (Unique Key table)</strong></th></tr>
</thead>
<tbody><tr><td><strong>BITMAP</strong></td><td>支持</td><td>不支持</td><td>支持。聚合函数必须为 bitmap_union、replace 或者 replace_if_not_null。</td><td>支持</td></tr><tr><td><strong>HLL</strong></td><td>支持</td><td>不支持</td><td>支持。聚合函数必须为 hll_union、replace 或者replace_if_not_null。</td><td>支持</td></tr><tr><td><strong>PERCENTILE</strong></td><td>支持</td><td>不支持</td><td>支持。聚合函数必须为 percentile_union、replace 或者 replace_if_not_null。</td><td>支持</td></tr><tr><td><strong>半结构化类型：</strong> <strong>JSON/ARRAY/MAP/STRUCT</strong></td><td>支持</td><td>支持</td><td>支持。聚合函数必须为 replace 或者 replace_if_not_null。</td><td>支持</td></tr></tbody>
</table>

## 数据变更

<table>
<thead>
<tr><th> </th><th width= "200"><strong>主键表 (Primary Key table)</strong></th><th><strong>明细表 (Duplicate Key table)</strong></th><th width= "200"><strong>聚合表 (Aggregate table)</strong></th><th><strong>更新表 (Unique Key table)</strong></th></tr>
</thead>
<tbody><tr><td><strong>导入数据时实现</strong> <strong>INSERT</strong></td><td rowspan="2">支持。[在导入任务中配置 <code>__op=0</code> 实现 INSERT](../../loading/Load_to_Primary_Key_tables.md)。内部实现时，StarRocks 将 INSERT 和 UPDATE 操作均视为 UPSERT 操作。</td><td>支持</td><td>支持（同聚合键值的数据行会聚合）</td><td>支持（同唯一键值的数据行会更新）</td></tr><tr><td><strong>导入数据时实现 UPDATE</strong></td><td>不支持</td><td>支持（使用 Replace 聚合函数实现）</td><td>支持（更新表本身就可以视为使用 Replace 聚合函数的聚合表）</td></tr><tr><td><strong>导入数据时实现 DELETE</strong></td><td>支持。[在导入任务中配置 <code>__op=1</code> 实现 DELETE](../../loading/Load_to_Primary_Key_tables.md)。</td><td>不支持</td><td>不支持</td><td>不支持</td></tr><tr><td><strong>导入数据列值的完整性</strong></td><td>默认必须导入全部列值。如果开启部分列更新<code>partial_update</code>，或者列具有默认值，则无需导入全部列值。</td><td>默认必须导入全部列值。如果列具有默认值，则无需导入全部列值。</td><td>默认必须导入全部列值。不过，聚合表可以通过指定 Value 列的聚合函数为 REPLACE_IF_NOT_NULL 实现部分列更新，具体使用方式，请参见 [aggr_type](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#column_definition)。并且如果列具有默认值，也无需导入全部列值。</td><td>默认必须导入全部列值。如果列具有默认值，则无需导入全部列值。</td></tr><tr><td><strong>[DML INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md)</strong></td><td colspan="4">支持</td></tr><tr><td><strong>[DML UPDATE](../../sql-reference/sql-statements/table_bucket_part_index/UPDATE.md)</strong></td><td><ul><li>Key 列作为过滤条件：支持</li><li>Value 列作为过滤条件：支持</li></ul></td><td colspan="3">不支持</td></tr><tr><td><strong>[DML DELETE](../../sql-reference/sql-statements/table_bucket_part_index/DELETE.md)</strong></td><td><ul><li>Key 列作为过滤条件：支持</li><li>Value 列作为过滤条件：支持</li></ul></td><td><ul><li>Key 列作为过滤条件：支持</li><li>Value 列作为过滤条件：支持。</li></ul>注意，仅支持基于 Key 或 Value  列本身的简单过滤条件，如 =、&lt;、&gt;，不支持复杂条件，如函数、子查询。</td><td  colspan="2"><ul><li>Key 列作为过滤条件：支持。注意，仅支持基于 Key 列本身的简单过滤条件，如 =、&lt;、&gt;，不支持复杂条件，如函数、子查询。</li><li>Value 列作为过滤条件：不支持</li></ul></td></tr></tbody>
</table>

## 和其他功能的兼容性

<table>
<thead>
<tr><th colspan="2"></th><th><strong>主键表 (Primary Key table)</strong></th><th><strong>明细表 (Duplicate Key table)</strong></th><th><strong>聚合表 (Aggregate table)</strong></th><th><strong>更新表 (Unique Key table)</strong></th></tr>
</thead>
<tbody><tr><td rowspan="2"><strong>Bitmap</strong> <strong>索引/Bloom filer 索引</strong></td><td><strong>基于 Key 列构建索引</strong></td><td colspan="4">支持</td></tr><tr><td><strong>基于 Value 列构建索引</strong></td><td>支持</td><td>支持</td><td>不支持</td><td>不支持</td></tr><tr><td rowspan="2"><strong>分区/分桶</strong></td><td><strong>表达式分区/List 分区</strong></td><td colspan="4">支持</td></tr><tr><td><strong>随机分桶</strong></td><td>不支持</td><td>自 3.1 起，支持</td><td>不支持</td><td>不支持</td></tr><tr><td  rowspan="2"><strong>物化视图</strong></td><td><strong>异步物化视图</strong></td><td  colspan="4">支持</td></tr><tr><td><strong>同步物化视图</strong></td><td>不支持</td><td>支持</td><td>支持</td><td>支持</td></tr><tr><td rowspan="2"><strong>其他功能</strong></td><td><strong>CTAS</strong></td><td>支持</td><td>支持</td><td>不支持</td><td>不支持</td></tr><tr><td><strong>Backup &amp; restore</strong></td><td>自 2.5 起，支持</td><td colspan="3">支持</td></tr></tbody>
</table>
