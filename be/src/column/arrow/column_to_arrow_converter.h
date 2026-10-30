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

#include <memory>

#include "column/column.h"
#include "common/status.h"

namespace arrow {

class Array;
class DataType;
class MemoryPool;

} // namespace arrow

namespace starrocks {

struct TypeDescriptor;

Status convert_column_to_arrow_array(const ColumnPtr& column, const TypeDescriptor& type_desc,
                                     const std::shared_ptr<arrow::DataType>& arrow_type, arrow::MemoryPool* pool,
                                     std::shared_ptr<arrow::Array>* result);

} // namespace starrocks
