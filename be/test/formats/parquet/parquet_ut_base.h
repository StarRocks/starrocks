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

#pragma once

#include <vector>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exec/hdfs_scanner.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"
#include "runtime/runtime_state.h"

namespace starrocks::parquet {

class ParquetUTBase {
public:
    static void create_conjunct_ctxs(ObjectPool* pool, RuntimeState* runtime_state, std::vector<TExpr>* tExprs,
                                     std::vector<ExprContext*>* conjunct_ctxs);

    static void append_smallint_conjunct(TExprOpcode::type opcode, SlotId slot_id, int value,
                                         std::vector<TExpr>* tExprs);
    static void append_int_conjunct(TExprOpcode::type opcode, SlotId slot_id, int value, std::vector<TExpr>* tExprs);
    static void append_bigint_conjunct(TExprOpcode::type opcode, SlotId slot_id, int64_t value,
                                       std::vector<TExpr>* tExprs);
    static void append_datetime_conjunct(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                         std::vector<TExpr>* tExprs);
    static void append_decimal_conjunct(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                        std::vector<TExpr>* tExprs);
    static void append_string_conjunct(TExprOpcode::type opcode, SlotId slot_id, std::string value,
                                       std::vector<TExpr>* tExprs);

    static void is_null_pred(SlotId slot_id, bool null, std::vector<TExpr>* tExprs);

    static void create_in_predicate_int_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                      std::set<int32_t>& values, std::vector<TExpr>* tExprs);

    static void create_in_predicate_string_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                         std::set<std::string>& values, std::vector<TExpr>* tExprs);

    static void create_in_predicate_date_conjunct_ctxs(TExprOpcode::type opcode, SlotId slot_id,
                                                       TPrimitiveType::type type, std::set<std::string>& values,
                                                       std::vector<TExpr>* tExprs);

    static void setup_conjuncts_manager(std::vector<ExprContext*>& conjuncts, const RuntimeFilterProbeCollector* rf,
                                        TupleDescriptor* tuple_desc, RuntimeState* runtime_state,
                                        HdfsScannerContext* params);

    static void create_dictmapping_string_conjunct(TExprOpcode::type opcode, SlotId slot_id, const std::string& value,
                                                   std::vector<TExpr>* tExprs);
};

} // namespace starrocks::parquet