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

#include "column/column.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"

namespace starrocks {

class RuntimeState;
class TExpr;
class TColumn;

// Utility functions for converting complex type default expressions to JSON strings
// This is used for persisting array/map/struct default values from FE to BE

/**
 * Convert TExpr (thrift expression) to JSON string for complex types (array/map/struct).
 * 
 * Flow:
 *   TExpr → ExprContext → prepare/open → evaluate → ColumnPtr → JSON string
 * 
 * This function creates a temporary RuntimeState internally for expression evaluation.
 * 
 * @param t_expr The thrift expression from FE (e.g., [], map{1:2}, row())
 * @return StatusOr<std::string> JSON string (e.g., "[1,2,3]", "{"k1":"v1"}", "{"f1":1,"f2":"abc"}")
 */
StatusOr<std::string> convert_default_expr_to_json_string(const TExpr& t_expr);

/**
 * Pre-process TColumn list: evaluate default_expr and fill default_value for complex types.
 * This should be called BEFORE creating TabletSchema from TColumn list.
 * 
 * This function creates a temporary RuntimeState internally for expression evaluation.
 * 
 * @param columns Mutable reference to TColumn vector (will be modified in-place)
 * @return Status OK if successful (errors are logged but don't fail the process)
 */
Status preprocess_default_expr_for_tcolumns(std::vector<TColumn>& columns);

} // namespace starrocks

