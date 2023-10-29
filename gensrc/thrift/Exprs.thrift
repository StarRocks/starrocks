// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Exprs.thrift

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp starrocks
namespace java com.starrocks.thrift

include "Types.thrift"
include "Opcodes.thrift"

enum TExprNodeType {
  // Be careful, to keep the compatibility between differen version fe and be,
  // please always add the new expr at last.
  AGG_EXPR,
  ARITHMETIC_EXPR,
  BINARY_PRED,
  BOOL_LITERAL,
  CASE_EXPR,
  CAST_EXPR,
  COMPOUND_PRED,
  DATE_LITERAL,
  FLOAT_LITERAL,
  INT_LITERAL,
  DECIMAL_LITERAL,
  IN_PRED,
  IS_NULL_PRED,
  LIKE_PRED,
  LITERAL_PRED,
  NULL_LITERAL,
  SLOT_REF,
  STRING_LITERAL,
  TUPLE_IS_NULL_PRED,
  INFO_FUNC,
  FUNCTION_CALL,

  // TODO: old style compute functions. this will be deprecated
  COMPUTE_FUNCTION_CALL,
  LARGE_INT_LITERAL,

  ARRAY_EXPR,
  ARRAY_ELEMENT_EXPR,
  ARRAY_SLICE_EXPR,

  TABLE_FUNCTION_EXPR,

  DICT_EXPR,
  PLACEHOLDER_EXPR,
  CLONE_EXPR,
  LAMBDA_FUNCTION_EXPR,
  SUBFIELD_EXPR,
  RUNTIME_FILTER_MIN_MAX_EXPR,
  MAP_ELEMENT_EXPR,
  BINARY_LITERAL,
  MAP_EXPR,
  DICT_QUERY_EXPR,

  // for inverted search
  MATCH_PRED,
}

struct TAggregateExpr {
  // Indicates whether this expr is the merge() of an aggregation.
  1: required bool is_merge_agg
}
struct TBoolLiteral {
  1: required bool value
}

struct TCaseExpr {
  1: required bool has_case_expr
  2: required bool has_else_expr
}

struct TDateLiteral {
  1: required string value
}

struct TFloatLiteral {
  1: required double value
}

struct TDecimalLiteral {
  1: required string value
  2: optional binary integer_value
}

struct TIntLiteral {
  1: required i64 value
}

struct TLargeIntLiteral {
  1: required string value
}

struct TBinaryLiteral {
  1: required binary value
}

struct TInPredicate {
  1: required bool is_not_in
}

struct TIsNullPredicate {
  1: required bool is_not_null
}

struct TLikePredicate {
  1: required string escape_char;
}

struct TLiteralPredicate {
  1: required bool value
  2: required bool is_null
}

struct TTupleIsNullPredicate {
  1: required list<Types.TTupleId> tuple_ids
}

struct TSlotRef {
  1: required Types.TSlotId slot_id
  2: required Types.TTupleId tuple_id
}

struct TPlaceHolder {
  1: optional bool nullable;
  2: optional i32 slot_id;
}

struct TStringLiteral {
  1: required string value;
}

struct TInfoFunc {
  1: required i64 int_value;
  2: required string str_value;
}

struct TFunctionCallExpr {
  // The aggregate function to call.
  1: required Types.TFunction fn

  // If set, this aggregate function udf has varargs and this is the index for the
  // first variable argument.
  2: optional i32 vararg_start_idx
}

struct TDictQueryExpr {
  1: required string db_name
  2: required string tbl_name
  3: required map<i64, i64> partition_version
  4: required list<string> key_fields
  5: required string value_field
  6: required bool strict_mode
}

// This is essentially a union over the subclasses of Expr.
struct TExprNode {
  1: required TExprNodeType node_type
  2: required Types.TTypeDesc type
  3: optional Opcodes.TExprOpcode opcode
  4: required i32 num_children

  5: optional TAggregateExpr agg_expr
  6: optional TBoolLiteral bool_literal
  7: optional TCaseExpr case_expr
  8: optional TDateLiteral date_literal
  9: optional TFloatLiteral float_literal
  10: optional TIntLiteral int_literal
  11: optional TInPredicate in_predicate
  12: optional TIsNullPredicate is_null_pred
  13: optional TLikePredicate like_pred
  14: optional TLiteralPredicate literal_pred
  15: optional TSlotRef slot_ref
  16: optional TStringLiteral string_literal
  17: optional TTupleIsNullPredicate tuple_is_null_pred
  18: optional TInfoFunc info_func
  19: optional TDecimalLiteral decimal_literal

  20: required i32 output_scale
  21: optional TFunctionCallExpr fn_call_expr
  22: optional TLargeIntLiteral large_int_literal

  23: optional i32 output_column
  24: optional Types.TColumnType output_type
  25: optional Opcodes.TExprOpcode vector_opcode
  // The function to execute. Not set for SlotRefs and Literals.
  26: optional Types.TFunction fn
  // If set, child[vararg_start_idx] is the first vararg child.
  27: optional i32 vararg_start_idx
  28: optional Types.TPrimitiveType child_type

  29: optional TPlaceHolder vslot_ref;

  // Used for SubfieldExpr
  30: optional list<string> used_subfield_names;
  31: optional TBinaryLiteral binary_literal;

  // For vector query engine
  50: optional bool use_vectorized  // Deprecated
  51: optional bool has_nullable_child
  52: optional bool is_nullable
  53: optional Types.TTypeDesc child_type_desc
  54: optional bool is_monotonic

  55: optional TDictQueryExpr dict_query_expr
}

struct TPartitionLiteral {
  1: optional Types.TPrimitiveType type
  2: optional TIntLiteral int_literal
  3: optional TDateLiteral date_literal
  4: optional TStringLiteral string_literal
}

// A flattened representation of a tree of Expr nodes, obtained by depth-first
// traversal.
struct TExpr {
  1: required list<TExprNode> nodes
}
