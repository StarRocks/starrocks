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

include "Status.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "Data.thrift"
include "Exprs.thrift"
include "InternalService.thrift"
include "DataSinks.thrift"
include "RuntimeProfile.thrift"

struct TKeyLiteralExpr {
    1: optional list<Exprs.TExpr> literal_exprs;
}

struct TExecShortCircuitParams {
    1: optional Descriptors.TDescriptorTable desc_tbl
    2: optional PlanNodes.TPlan plan // scan node, or project + scan node, or values
    3: optional list<Exprs.TExpr> output_exprs
    4: optional DataSinks.TDataSink data_sink; // result sink if not set
    5: optional bool is_binary_row;
    6: optional bool enable_profile;
    7: optional PlanNodes.TScanRange scan_range;
    8: optional list<TKeyLiteralExpr> key_literal_exprs;
    9: optional list<i64> tablet_ids;
    10: optional list<string> versions;
}
