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
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Data.thrift

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

// Serialized, self-contained version of a RowBatch (in be/src/runtime/row-batch.h).
struct TRowBatch {
  // total number of rows contained in this batch
  1: required i32 num_rows

  // row composition
  2: required list<Types.TTupleId> row_tuples

  // There are a total of num_rows * num_tuples_per_row offsets
  // pointing into tuple_data.
  // An offset of -1 records a NULL.
  3: list<i32> tuple_offsets

  // binary tuple data
  // TODO: figure out how we can avoid copying the data during TRowBatch construction
  4: string tuple_data

  // Indicates whether tuple_data is snappy-compressed
  5: bool is_compressed

  // backend num, source
  6: i32 be_number
  // packet seq
  7: i64 packet_seq
}

// this is a union over all possible return types
struct TColumnValue {
  // TODO: use <type>_val instead of camelcase
  1: optional bool boolVal
  2: optional i32 intVal
  3: optional i64 longVal
  4: optional double doubleVal
  5: optional string stringVal
}

struct TResultRow {
  1: list<TColumnValue> colVals
}

// Serialized, self-contained version of a RowBatch (in be/src/runtime/row-batch.h).
struct TResultBatch {
  // mysql result row
  1: required list<binary> rows

  // Indicates whether tuple_data is snappy-compressed
  2: required bool is_compressed

  // packet seq used to check if there has packet lost
  3: required i64 packet_seq
  
  // For mark statistic data version
  10: optional i32 statistic_version
}

struct TGlobalDict {
    1: optional i32 columnId
    2: optional list<binary> strings
    3: optional list<i32> ids
    4: optional i64 version
}

// Statistic data for new planner 
struct TStatisticData {
    1: optional string updateTime
    2: optional i64 dbId
    3: optional i64 tableId
    4: optional string columnName
    5: optional i64 rowCount
    6: optional double dataSize
    7: optional i64 countDistinct
    8: optional i64 nullCount
    9: optional string max
    10: optional string min
    11: optional string histogram
    // global dict for low cardinality string
    12: optional TGlobalDict dict
    // the latest partition version for this table
    13: optional i64 meta_version
    14: optional i64 partitionId
    // the batch load version
    15: optional binary hll
    16: optional string partitionName
}

// Result data for user variable
struct TVariableData {
    1: optional bool isNull
    2: optional binary result
}
