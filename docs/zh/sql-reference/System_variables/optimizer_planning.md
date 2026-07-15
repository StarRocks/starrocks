---
displayed_sidebar: docs
sidebar_label: "查询优化器与规划"
sidebar_position: 1
description: "控制基于成本的优化器、统计信息驱动的规划、剪枝、重写以及 CTE 重用的会话变量。"
---

# 系统变量 - 查询优化器与规划

如需了解如何查看和设置变量，请参见 [系统变量概述](../System_variable.md)。

### array_low_cardinality_optimize

* **作用域**: Session
* **描述**: 控制优化器是否将 `ARRAY<VARCHAR>` 列纳入低基数（基于字典）的解码及相关优化的考虑范围。启用时，优化器的低基数规则（例如 `DecodeCollector`）可能会定义字典列，并将字典解码应用于类型为 VARCHAR 或 `ARRAY<VARCHAR>` 的表达式。禁用时，仅标量 VARCHAR 列有资格参与，`ARRAY<VARCHAR>` 类型会被这些低基数优化忽略。该变量由 `DecodeCollector.supportAndEnabledLowCardinality(...)` 读取以控制对数组的支持，并通过 `SessionVariable` 的 getter/setter 方法暴露。
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: v3.3.0, v3.4.0, v3.5.0

### cbo_cte_force_reuse_limit_without_order_by

* **描述**: 控制优化器如何处理定义中包含 `LIMIT` 但没有 `ORDER BY` 的公用表表达式（CTE）。为 `true`（默认）时，优化器（`ForceCTEReuseRule`）会强制重用此类 CTE，使每个消费者读取相同的行，从而避免因重新计算无序的 `LIMIT` 而产生的不确定结果。为 `false` 时，优化器可能会内联该 CTE，这可能导致各消费者之间结果不一致。
* **范围**: Session
* **默认值**: `true`
* **数据类型**: Boolean
* **引入版本**: v3.5.10、v4.0.3、v4.1.0

### cbo_cte_force_reuse_node_count

* **描述**: 会话级阈值，用于控制优化器对公用表表达式（CTE）的一种快捷处理。在 RelationTransformer.visitCTE 中，规划器会统计 CTE 生产者树中的节点数（cteContext.getCteNodeCount）。如果该计数大于或等于此阈值且阈值大于 0，则 transformer 强制重用该 CTE：跳过对生产者计划的内联/转换，构建一个带有预计算表达式映射（无输入）的 consume 操作符，并改用生成的列引用。这样可以在面对非常大的 CTE 生产者树时减少优化器时间，但可能以生成的物理计划不是最优为代价。将该值设置为 `0` 可禁用强制重用优化。此变量在 SessionVariable 中有 getter/setter，并按会话应用。
* **范围**: Session
* **默认值**: `2000`
* **数据类型**: int
* **引入版本**: v3.5.3

### cbo_cte_max_limit

* **描述**: 限制优化器在一个查询中考虑进行 CTE 重用的公用表表达式（CTE）数量。规划期间该值会传递给 CTE transformer（`CTETransformerContext`）；当查询包含的 CTE 数量超过此限制时，会回退到内联而不是基于重用的计划。提高该值可让优化器对包含更多 CTE 的查询应用 CTE 重用，但会增加规划时间。
* **范围**: Session
* **默认值**: `10`
* **数据类型**: int
* **引入版本**: v2.5.0

### cbo_cte_reuse

* **描述**: 控制优化器是否可以通过重用 Common Table Expression (CTE) 重写 multi-distinct 聚合查询（CBO 的 CTE‑reuse 重写）。启用时，Planner ）可能会为多列 DISTINCT、偏斜聚合或当统计信息表明 CTE 重写更高效时选择基于 CTE 的重写；此配置项也会尊重 `prefer_cte_rewrite` hint。禁用时，不允许基于 CTE 的重写，Planner 将尝试 multi-function 重写；如果查询需要 CTE（例如，多列 DISTINCT 或 multi-function 重写无法处理的函数），Planner 将抛出错误。注意：只有在 Pipeline Engine 打开时 CTE 重用才生效。
* **默认值**: `true`
* **数据类型**: Boolean
* **引入版本**: v3.2.0

### cbo_cte_reuse_rate

* **描述**: 控制优化器用于判断重用公用表表达式（CTE）是否比内联更划算的成本比率。在基于成本判断时，该值越大，CTE 重用的估算成本越高（在 `CostModel` 中，CTE anchor 的内存成本估算为 `produce_size * (1 + rate)`），优化器越不倾向于重用该 CTE。该值还有两个特殊取值：
  * `< 0`：禁用 CTE 重用，始终内联 CTE。
  * `0`：始终重用 CTE。
  * `> 0`：根据成本（按此比率加权）在重用与内联之间选择。

  内部实现名称为 `cbo_cte_reuse_rate_v2`；`cbo_cte_reuse_rate` 是 `SHOW VARIABLES` 中显示以及用于设置该变量的名称。
* **范围**: Session
* **默认值**: `1.15`
* **数据类型**: double
* **引入版本**: v2.2.0

### cbo_disabled_rules

* **描述**: 以逗号分隔的优化器规则名称列表，用于在当前会话中禁用规则。每个名称必须与 `RuleType` 枚举值匹配，且仅可禁用名称以 `TF_`（转换规则）或 `GP_`（组组合规则）开头的规则。该会话变量存储在 `SessionVariable`（`getCboDisabledRules` / `setCboDisabledRules`）中，并由优化器通过 `OptimizerOptions.applyDisableRuleFromSessionVariable()` 应用；该方法解析列表并清除对应的规则开关，从而在规划期间跳过这些规则。通过 SET 语句设置时，值会被校验，服务器会以明确错误信息拒绝未知名称或不以 `TF_`/`GP_` 开头的名称（例如 "Unknown rule name(s): ..." 或 "Only TF_ ... and GP_ ... can be disabled"）。在规划器运行时，未知的规则名称会被忽略并记录警告（记录为 "Ignoring unknown rule name: ... (may be from different version)"）。名称必须与枚举标识符完全匹配（区分大小写）。名称周围的空白会被修剪；空条目会被忽略。
* **作用域**: Session
* **默认值**: `""`（无被禁用规则）
* **数据类型**: String
* **引入版本**: -

### cbo_enable_low_cardinality_optimize

* 描述：是否开启低基数全局字典优化。开启后，查询 STRING 列时查询速度会有 3 倍左右提升。
* 默认值：true

### cbo_eq_base_type

* 描述：用来指定 DECIMAL 类型和 STRING 类型的数据比较时的强制类型，默认按照 `DECIMAL` 类型进行比较，可选 `VARCHAR`（按数值进行比较）。**该变量仅在进行 `=` 和 `!=` 比较时生效。**
* 类型：String
* 引入版本：v2.5.14

### cbo_json_v2_dict_opt

* 描述：是否为 JSON v2 路径改写生成的 Flat JSON 字符串子列启用低基数字典优化。开启后，优化器可以为这些子列构建并使用全局字典，从而加速字符串表达式、GROUP BY、JOIN 等操作。
* 默认值：true
* 类型：Boolean

### cbo_json_v2_rewrite

* 描述：是否在优化器中启用 JSON v2 路径改写。开启后，会将 JSON 函数（如 `get_json_*`）改写为直接访问 Flat JSON 子列，从而启用谓词下推、列裁剪以及字典优化。
* 默认值：true
* 类型：Boolean

### cbo_max_reorder_node_use_dp

* **描述**: 会话级限制，用于控制 CBO 何时包含 DP（dynamic programming，动态规划）连接重排序算法。优化器将连接输入数量（MultiJoinNode.atoms.size()）与此值比较，只有在 `multiJoinNode.getAtoms().size()` 小于等于 `cbo_max_reorder_node_use_dp` 且启用了 `cbo_enable_dp_join_reorder` 时才运行或添加 DP 重排序。用于 将 JoinReorderDP 添加到候选算法以及在将计划复制到 memo 时决定是否执行 JoinReorderDP。默认值 10 反映了一个实际性能的截止点。可通过调优该值在优化器运行时长（DP 开销较大）与对更大多表连接查询潜在计划质量之间进行权衡。该设置与 `cbo_enable_dp_join_reorder` 和贪心阈值 `cbo_max_reorder_node_use_greedy` 交互。比较为包含等号（`<=`）。
* **范围**: Session
* **默认值**: `10`
* **数据类型**: long
* **引入版本**: v3.2.0

### cbo_max_reorder_node_use_exhaustive

* **范围**: Session
* **描述**: 控制 CBO 中 Join 重排序算法选择的阈值。优化器会统计查询中的内/交叉连接节点数量；当该计数大于此值时，planner 会走基于变换（更激进）的重排序路径：强制收集 CTE 统计信息并调用 ReorderJoinRule.transform 及相关的交换律规则。 当计数小于或等于此值时，planner 应用开销较小的连接变换规则（并且在某些半/反连接情况下可能会添加 INNER_JOIN_LEFT_ASSCOM_RULE）。优化器（`SPMOptimizer`, `QueryOptimizer`）会读取此会话变量，并且可以通过 `setMaxTransformReorderJoins` 在会话级别设置。
* **默认值**: `4`
* **数据类型**: int
* **引入版本**: v3.2.0

### cbo_max_reorder_node_use_greedy

* **描述**: 在 multi-join 中，CBO 优化器会考虑使用贪婪（greedy）join-reorder 算法的最大联接输入（atoms）数量。优化器在构建候选重排序算法列表时会检查此项以及 `cbo_enable_greedy_join_reorder`：如果 `multiJoinNode.getAtoms().size()` 小于或等于此值，则会添加并执行一个 `JoinReorderGreedy` 实例。该变量在会话范围生效，影响是否尝试贪婪重排序（前提是有统计信息且启用了贪婪算法）。调整此值以控制在有大量被联接关系的查询中优化器的时间/复杂度权衡。
* **范围**: Session
* **默认值**: `16`
* **类型**: Int
* **引入版本**: v3.4.0, v3.5.0

### cbo_use_correlated_predicate_estimate

* **描述**: 用于控制优化器在估算跨多列的合取相等谓词的选择性时，是否应用考虑相关性的启发式方法。当启用（默认）时，估算器会对主多列统计或最具选择性的谓词之外的附加列的选择性应用指数衰减权重，从而减少后续谓词的乘法影响（权重：对于最多三个附加列分别为 0.5、0.25、0.125）。当禁用时，不应用衰减（decay factor = 1），估算器会对这些列使用完整选择性相乘（更强的独立性假设）。StatisticsEstimateUtils.estimateConjunctiveEqualitySelectivity 会检查此标志，以在多列统计路径和回退路径中选择衰减因子，从而影响 CBO 使用的基数估算。
* **范围**: Session
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: v3.5.0

### enable_cbo_table_prune

* **描述**: 启用后，优化器将在 memo 优化期间添加基于代价的表裁剪规则（CboTablePruneRule），对保持基数的连接进行基于代价的表裁剪。该规则仅在优化器中有条件地添加（参见 QueryOptimizer.memoOptimize 和 SPMOptimizer.memoOptimize），并且只在连接树中的连接节点数量较小时添加（少于 10 个连接节点）。此选项补充基于规则的裁剪开关 `enable_rbo_table_prune`，允许基于代价的优化器尝试从连接处理中移除不必要的表或输入，以减少计划和执行复杂度。默认关闭，因为裁剪可能改变计划形状；仅在代表性工作负载上验证后再启用。
* **范围**: Session
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_reduce_cast_varchar_length_inheritance (global)

* 描述：当 `ReduceCastRule` 消除同类型的 `VARCHAR -> VARCHAR` cast 时，是否保留目标 `VARCHAR(N)` 的长度信息。开启后，可使 `CAST(col AS VARCHAR(N))` 这类语句在 prepare 和 execute 阶段返回一致的结果集元数据。
* 默认值：false
* 数据类型：Boolean
* 引入版本：v3.5.16、v4.0.9

### enable_reduce_cast_varchar_expr_sync_type (global)

* 描述：当 `ReduceCastRule` 消除同类型的 `VARCHAR -> VARCHAR` cast 后，是否将复用的 planner `Expr` 的 `type` 和 `originType` 同步为改写后的 `VARCHAR(N)` 类型。
* 默认值：true
* 数据类型：Boolean
* 引入版本：v3.5.16、v4.0.9

### enable_plan_advisor

* 描述：是否为慢查询或手动标记查询开启 Query Feedback 功能。
* 默认值：true
* 引入版本：v3.4.0

### enable_predicate_reorder

* **范围**: Session
* **描述**: 启用后，优化器在逻辑/物理计划重写阶段对 AND（合取）谓词应用 Predicate Reorder 规则。该规则通过 `Utils.extractConjuncts` 提取合取项，使用 `DefaultPredicateSelectivityEstimator` 估算每个合取项的选择性，并按估算选择性升序（较不限制的先）重排合取项以构造新的 `CompoundPredicateOperator`（AND）。只有当操作符包含超过一个合取项的 `CompoundPredicateOperator` 时，规则才会运行。当可用时，统计信息从子 `OptExpression` 的统计信息收集；对于 `PhysicalOlapScanOperator`，它会从 `GlobalStateMgr.getCurrentState().getStatisticStorage()` 获取列统计。如果缺少子统计且扫描不是 OLAP 扫描，则规则将跳过重排序。该会话变量通过 `SessionVariable.isEnablePredicateReorder()` 暴露，并提供辅助方法 `enablePredicateReorder()` 与 `disablePredicateReorder()`。
* **默认值**: false
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_query_trigger_analyze

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否开启查询触发 ANALYZE 外表任务。
* 引入版本：v3.4.0

### enable_rbo_table_prune

* **描述**: 启用后，优化器在当前会话中对保持基数的连接应用基于规则的 (RBO) 表裁剪。优化器运行一系列重写和裁剪步骤（分区裁剪、投影合并/拆分、`UniquenessBasedTablePruneRule`、连接重排 和 `RboTablePruneRule`）以移除不必要的表扫描备选项并减少连接中的扫描分区/行数。启用此选项还会在逻辑规则重写运行时禁用连接等价推导（`context.setEnableJoinEquivalenceDerive(false)`），以避免冲突的转换。如果设置了 `enable_table_prune_on_update`，裁剪流程在处理 UPDATE 语句时可能额外运行 `PrimaryKeyUpdateTableRule`。该规则仅在查询包含可裁剪连接时执行（通过 `Utils.hasPrunableJoin(tree)` 检查）。
* **范围**: Session
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_spm_rewrite

* 描述：是否启用 SQL Plan Manager (SPM) 查询改写功能。启用此功能后，StarRocks 将自动将相应的查询改写为绑定的查询计划，以提升查询性能和稳定性。
* 默认值：false

### enable_table_prune_on_update

* **范围**: Session
* **描述**: 控制优化器是否为 UPDATE 语句应用针对主键的表裁剪规则。启用时，QueryOptimizer（在 pruneTables 阶段）会调用 `PrimaryKeyUpdateTableRule` 来重写/裁剪更新计划 —— 可能在主键更新模式下改进裁剪效果。该标志仅在基于规则/成本模型（rule-based/CBO）表裁剪启用时生效（参见 `enable_rbo_table_prune`）。默认禁用，因为该转换可能会改变数据布局/计划形态（例如 OlapTableSink 的 bucket-shuffle 布局），并可能在并发更新时导致正确性或性能回退。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.4

### enable_ukfk_opt

* **描述**: 启用优化器对 Unique-Key / Foreign-Key (UK/FK) 基于的转换和统计增强的支持。设置后，优化器会运行 `UKFKConstraintsCollector` 自底向上收集唯一键和外键约束并将其附加到计划节点（OptExpressions）。收集到的约束将被像 `PruneUKFKJoinRule`（可以裁剪连接的 UK 侧、将谓词从 UK 列重写到 FK 列并为外连接场景添加 IS NULL 检查）和 `PruneUKFKGroupByKeysRule`（可以删除由 UK/FK 关系派生的冗余 GROUP BY 键）等转换规则使用。收集的 UK/FK 信息也被 `StatisticsCalculator` 使用，以为 UK‑FK 连接生成更紧的连接基数估计，并在更精确时替代默认估计。默认关闭（`false`），因为这些优化依赖于声明性的模式约束，并且可能改变计划形状和谓词放置。
* **默认值**: `false`
* **范围**: Session
* **数据类型**: boolean
* **引入版本**: v3.2.4

### low_cardinality_optimize_on_lake

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否在数据湖查询中启用低基数优化。有效值：
  * `true`（默认）：在数据湖查询中启用低基数优化。
  * `false`: 在数据湖查询中禁用低基数优化。
* 引入版本：v3.5.0

### low_cardinality_optimize_v2

* **作用域**: Session
* **描述**: 会话级变量，用于选择优化器应用哪种低基数（low-cardinality）优化重写。为 `true` 时，优化器将尝试由 `LowCardinalityRewriteRule` 实现的新版 Skew Join V2 重写（使用 `DecodeCollector` / `DecodeRewriter` 对低基数 VARCHAR 列进行编码/解码）。为 `false` 时，优化器回退到由 `AddDecodeNodeForDictStringRule` 实现的旧版重写。执行任一重写还需要 `enableLowCardinalityOptimize` 被启用；如果 `enableLowCardinalityOptimize` 被禁用，则不执行任何低基数重写。该变量在优化器的树重写路径中被检查以选择合适的转换。
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: v3.3.0, v3.4.0, v3.5.0

### new_planner_optimize_timeout

* 描述：查询优化器的超时时间。一般查询中 Join 过多时容易出现超时。超时后会报错并停止查询，影响查询性能。您可以根据查询的具体情况调大该参数配置，也可以将问题上报给 StarRocks 技术支持进行排查。
* 默认值：3000
* 单位：毫秒

### plan_mode

* 描述：Iceberg Catalog 元数据获取方案模式。详细信息，参考 [Iceberg Catalog 元数据获取方案](../data_source/catalog/iceberg/iceberg_catalog.md#附录元数据周期性后台刷新方案)。有效值：
  * `auto`：系统自动选择方案。
  * `local`：由 FE 在本地解析 Iceberg manifest 文件，并在解析过程中将 scan range 增量下发给 BE，无需等待所有 manifest 解析完成，可降低内存占用和首包延迟。
  * `distributed`：将 manifest 解析任务分发给多个 BE 并行处理，但 FE 需等待所有 BE 返回结果后才能下发 scan range，对于 manifest 文件较多的大表，可能导致较高内存占用和较长等待时间。仅在 FE CPU 成为瓶颈且 manifest 数量极多时建议使用。
* 默认值：local（v3.5 起由 `auto` 改为 `local`；v3.5 起增量 scan range 下发默认开启，`local` 模式在大多数场景下内存占用更低、延迟更小）
* 引入版本：v3.3.3

#### enable_iceberg_column_statistics

* 描述：是否获取列统计信息，例如 `min`、`max`、`null count`、`row size` 和 `ndv`（如果存在 puffin 文件）。当此项设置为 `false` 时，仅收集行数信息。
* 默认值：false
* 引入版本：v3.4

### prefer_cte_rewrite

* **描述**: 控制优化器是否优先对 multi-distinct 聚合查询采用基于公用表表达式（CTE）的重写。为 `true` 且同时启用了 `cbo_cte_reuse` 时，`RewriteMultiDistinctRule` 会为包含多个 distinct 聚合的查询选择基于 CTE 的重写，而不是 multi-function 重写。为 `false`（默认）时，优化器依据常规的成本/统计信息启发式规则在 CTE 重写与 multi-function 重写之间做出选择。
* **范围**: Session
* **默认值**: `false`
* **数据类型**: Boolean
* **引入版本**: v3.2.0

### range_pruner_max_predicate

* 描述：设置进行 Range 分区裁剪时，最多能使用的 IN 谓词的个数，默认值：100。如果超过该值，会扫描全部 tablet，降低查询性能。
* 默认值：100
* 引入版本：v3.0

