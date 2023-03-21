namespace cpp starrocks
namespace java com.starrocks.thrift

include "Exprs.thrift"
include "Types.thrift"
include "Opcodes.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"
include "RuntimeFilter.thrift"
include "PlanNodes.thrift"

struct TNormalOlapScanNode {
  1: optional i64 tablet_id;
  2: optional i64 index_id;
  3: optional list<i32> remapped_slot_ids;
  4: optional list<string> selected_column;
  5: optional list<string> key_column_names;
  6: optional list<Types.TPrimitiveType> key_column_types;
  7: optional bool is_preaggregation;
  8: optional string sort_column;
  9: optional string rollup_name;
  10: optional list<i32> dict_string_ids;
  11: optional list<i32> dict_int_ids;
  12: optional list<string> unused_output_column_name;
  13: optional list<i64> selected_partition_ids;
  14: optional list<i64> selected_partition_versions;
}

struct TNormalProjectNode {
  1: optional list<Types.TSlotId> slot_ids;
  2: optional list<binary> exprs;
  3: optional list<Types.TSlotId> cse_slot_ids;
  4: optional list<binary> cse_exprs;
}

struct TNormalAggregationNode {
  1: optional list<binary> grouping_exprs
  2: optional list<binary> aggregate_functions
  3: optional Types.TTupleId intermediate_tuple_id
  4: optional Types.TTupleId output_tuple_id
  5: optional bool need_finalize
  6: optional bool use_streaming_preaggregation
  7: optional i32 agg_func_set_version = 1
  8: optional bool has_outer_join_child
  9: optional PlanNodes.TStreamingPreaggregationMode streaming_preaggregation_mode 
}

struct TNormalDecodeNode {
  1: optional list<i32> from_dict_ids
  2: optional list<i32> to_string_ids
  3: optional list<Types.TSlotId> slot_ids
  4: optional list<binary> string_functions
}


struct TNormalHashJoinNode {
  1: optional PlanNodes.TJoinOp join_op
  2: optional list<binary> eq_join_conjuncts
  3: optional list<binary> other_join_conjuncts
  4: optional bool is_rewritten_from_not_in
  5: optional PlanNodes.TJoinDistributionMode distribution_mode
  6: optional list<binary> partition_exprs
  7: optional list<Types.TSlotId> output_columns
}


struct TNormalNestLoopJoinNode {
  1: optional PlanNodes.TJoinOp join_op
  2: optional list<binary> join_conjuncts
}


struct TNormalTableFunctionNode {
  1: optional Types.TFunction table_function
  2: optional list<Types.TSlotId> param_columns
  3: optional list<Types.TSlotId> outer_columns
  4: optional list<Types.TSlotId> fn_result_columns
}

struct TNormalRepeatNode {
  1: optional Types.TTupleId output_tuple_id
  2: optional list<list<Types.TSlotId>> slot_id_set_list
  3: optional list<i64> repeat_id_list
  4: optional list<list<i64>> grouping_list
  5: optional list<Types.TSlotId> all_slot_ids
}

struct TNormalAnalyticNode {
  1: optional list<binary> partition_exprs
  2: optional list<binary> order_by_exprs
  3: optional list<binary> analytic_functions
  4: optional PlanNodes.TAnalyticWindow window
  5: optional Types.TTupleId intermediate_tuple_id
  6: optional Types.TTupleId output_tuple_id
  7: optional Types.TTupleId buffered_tuple_id
  8: optional binary partition_by_eq
  9: optional binary order_by_eq
  10: optional bool has_outer_join_child
}

struct TNormalAssertNumRowsNode {
  1: optional i64 desired_num_rows
  2: optional PlanNodes.TAssertion assertion
}

struct TNormalSortInfo {
  1: optional list<binary> ordering_exprs
  2: optional list<bool> is_asc_order
  3: optional list<bool> nulls_first
  4: optional list<binary> sort_tuple_slot_exprs
}

struct TNormalExchangeNode {
  1: optional list<Types.TTupleId> input_row_tuples
  2: TNormalSortInfo sort_info
  3: optional i64 offset
  4: optional Partitions.TPartitionType partition_type
}

struct TNormalSortNode {
  1: optional TNormalSortInfo sort_info
  2: optional bool use_top_n
  3: optional i64 offset
  4: optional bool is_default_limit
  5: optional bool has_outer_join_child
  6: optional list<binary> analytic_partition_exprs
  7: optional list<binary> partition_exprs
  8: optional i64 partition_limit
  9: optional PlanNodes.TTopNType topn_type
}

struct TNormalSortAggregationNode {
  1: optional list<binary> grouping_exprs
  2: optional list<binary> aggregate_functions
  3: optional i32 agg_func_set_version
}

struct TNormalSetOperationNode {
  1: optional Types.TTupleId tuple_id
  2: optional list<list<binary>> result_expr_lists
  3: optional list<list<binary>> const_expr_lists
  4: optional i64 first_materialized_child_idx
}

struct TNormalPlanNode {
  1: optional Types.TPlanNodeId node_id
  2: optional PlanNodes.TPlanNodeType node_type
  3: optional i32 num_children
  4: optional i64 limit
  5: optional list<Types.TTupleId> row_tuples
  6: optional list<bool> nullable_tuples
  7: optional list<binary> conjuncts

  8: optional TNormalAggregationNode agg_node
  9: optional TNormalOlapScanNode olap_scan_node
  10: optional TNormalProjectNode project_node
  11: optional TNormalDecodeNode decode_node
  12: optional TNormalHashJoinNode hash_join_node
  13: optional TNormalNestLoopJoinNode nestloop_join_node
  14: optional TNormalTableFunctionNode table_function_node
  15: optional TNormalRepeatNode repeat_node
  16: optional TNormalAnalyticNode analytic_node
  17: optional TNormalAssertNumRowsNode assert_num_rows_node
  18: optional TNormalExchangeNode exchange_node
  19: optional TNormalSortNode sort_node
  20: optional TNormalSortAggregationNode sort_aggregation_node
  22: optional TNormalSetOperationNode set_operation_node
}
