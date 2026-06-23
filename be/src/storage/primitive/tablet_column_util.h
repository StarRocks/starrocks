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

#include <cstdint>
#include <string>

#include "base/status.h"
#include "base/statusor.h"

namespace starrocks {

class ColumnPB;
class TColumn;
class TExpr;

void convert_to_new_version(TColumn* tcolumn);

Status t_column_to_pb_column(int32_t unique_id, const TColumn& t_column, ColumnPB* column_pb);

StatusOr<std::string> convert_default_expr_to_json_string(const TExpr& t_expr);

} // namespace starrocks
