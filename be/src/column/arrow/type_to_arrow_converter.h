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

// This file contains code originally based on Apache Doris'
// be/src/util/arrow/row_batch.h under the Apache License 2.0.

#pragma once

#include <memory>
#include <string>

#include "common/status.h"
#include "types/type_descriptor.h"

namespace arrow {
class DataType;
class Field;
} // namespace arrow

namespace starrocks {

Status convert_to_arrow_type(const TypeDescriptor& type, std::shared_ptr<arrow::DataType>* result);
Status convert_to_arrow_field(const TypeDescriptor& desc, const std::string& col_name, bool is_nullable,
                              std::shared_ptr<arrow::Field>* field);

Status convert_to_arrow_type_for_flight_sql(const TypeDescriptor& type, std::shared_ptr<arrow::DataType>* result);
Status convert_to_arrow_field_for_flight_sql(const TypeDescriptor& desc, const std::string& col_name, bool is_nullable,
                                             std::shared_ptr<arrow::Field>* field, int32_t version);

} // namespace starrocks
