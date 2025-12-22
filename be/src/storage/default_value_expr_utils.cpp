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

#include "storage/default_value_expr_utils.h"

#include <chrono>
#include <ctime>

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/cast_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Helper function to create a minimal RuntimeState for constant expression evaluation
static std::unique_ptr<RuntimeState> create_temp_runtime_state() {
    TUniqueId dummy_query_id;
    dummy_query_id.hi = 0;
    dummy_query_id.lo = 0;
    
    TQueryOptions query_options;
    // Use default query options - safe for constant expression evaluation
    
    TQueryGlobals query_globals;
    // Set reasonable defaults for constant evaluation
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    
    std::tm tm_buf;
    localtime_r(&now_time_t, &tm_buf);
    char time_str[64];
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_buf);
    
    query_globals.now_string = time_str;
    query_globals.timestamp_ms = now_ms;
    query_globals.time_zone = "UTC";  // Default to UTC for DDL
    
    auto state = std::make_unique<RuntimeState>(dummy_query_id, query_options, query_globals, 
                                                 ExecEnv::GetInstance());
    state->init_instance_mem_tracker();
    
    return state;
}

StatusOr<std::string> convert_default_expr_to_json_string(const TExpr& t_expr) {
    LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Starting TExpr to JSON string conversion, "
               << "expr nodes count: " << t_expr.nodes.size();
    
    // Log each node in the TExpr tree to see if there's a Cast
    for (size_t i = 0; i < t_expr.nodes.size(); i++) {
        const auto& node = t_expr.nodes[i];
        LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] TExpr node[" << i << "]: "
                   << "node_type=" << static_cast<int>(node.node_type)
                   << ", num_children=" << node.num_children;
    }
    
    // Create a temporary RuntimeState for expression evaluation
    auto state = create_temp_runtime_state();
    
    // Use a dedicated ObjectPool for expression evaluation
    ObjectPool pool;
    
    // Step 1: TExpr → ExprContext
    ExprContext* ctx = nullptr;
    RETURN_IF_ERROR(Expr::create_expr_tree(&pool, t_expr, &ctx, state.get()));
    
    if (ctx == nullptr || ctx->root() == nullptr) {
        LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Failed to create expression tree from TExpr";
        return Status::InternalError("Failed to create expression tree from TExpr");
    }
    
    LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Expression tree created successfully, "
               << "root expr type: " << ctx->root()->type().debug_string();
    
    // Step 2: prepare() - initialize expression
    RETURN_IF_ERROR(ctx->prepare(state.get()));
    LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Expression prepared successfully";
    
    // Step 3: open() - finalize expression
    RETURN_IF_ERROR(ctx->open(state.get()));
    LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Expression opened successfully";
    
    // Step 4: evaluate_const() - evaluate constant expression (more appropriate for default values)
    // Default value expressions are always constants: row(1, 'abc'), [1,2,3], map{1:2}, etc.
    ColumnPtr column;
    auto eval_result = ctx->root()->evaluate_const(ctx);
    if (!eval_result.ok()) {
        LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Failed to evaluate constant expression: "
                   << eval_result.status().to_string();
        ctx->close(state.get());
        return eval_result.status();
    }
    column = eval_result.value();
    
    if (column == nullptr || column->size() == 0) {
        LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Expression evaluation returned empty result";
        ctx->close(state.get());
        return Status::InternalError("Failed to evaluate default expression: empty result");
    }
    
    LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Expression evaluated successfully, "
               << "column size: " << column->size()
               << ", is_constant: " << column->is_constant();
    
    // Unwrap ConstColumn if needed (default values are typically wrapped in ConstColumn)
    if (column->is_constant()) {
        auto const_col = down_cast<const ConstColumn*>(column.get());
        column = const_col->data_column()->clone();  // Clone to get a mutable copy
        LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Unwrapped ConstColumn, new size: " << column->size();
    }
    
    // Ensure we only have one value (constants should produce exactly 1 value)
    if (column->size() != 1) {
        LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Column has " << column->size() 
                   << " rows, keeping only the first value";
        // Keep only the first value by creating a new single-element column
        Datum first_datum = column->get(0);
        auto single_value_column = column->clone_empty();
        // Use const_cast since we know single_value_column is mutable (just created)
        const_cast<Column*>(single_value_column.get())->append_datum(first_datum);
        column = std::move(single_value_column);
    }
    
    // Step 5: Column → JSON string (convert the single value at index 0)
    ASSIGN_OR_RETURN(auto json_str, cast_type_to_json_str(column, 0));
    
    LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Successfully converted to JSON string, "
               << "length: " << json_str.length() 
               << ", JSON: " << json_str;
    
    // Cleanup: close expression context and free resources
    ctx->close(state.get());
    
    return json_str;
}

Status preprocess_default_expr_for_tcolumns(std::vector<TColumn>& columns) {
    for (auto& column : columns) {
        if (column.__isset.default_expr) {
            LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Processing column: " << column.column_name
                      << ", expr nodes: " << column.default_expr.nodes.size();
            
            auto result = convert_default_expr_to_json_string(column.default_expr);
            if (result.ok()) {
                column.default_value = result.value();
                column.__isset.default_value = true;
                LOG(ERROR) << "[DEFAULT_EXPR_DEBUG] Successfully converted default_expr to JSON for column: "
                          << column.column_name << ", JSON: " << column.default_value;
            } else {
                LOG(WARNING) << "[DEFAULT_EXPR_DEBUG] Failed to convert default_expr for column: "
                            << column.column_name << ", error: " << result.status().to_string()
                            << ". Column will use NULL or nullable behavior.";
                // Don't fail the entire process - just skip this column
            }
        }
    }
    
    return Status::OK();
}

} // namespace starrocks

