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

#include <string>

#include "column/column.h"
#include "common/status.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks {

Status add_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                         simdjson::ondemand::value* value);

Status add_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                              simdjson::ondemand::value* value);
Status add_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                              simdjson::ondemand::object* value);
Status add_binary_column_from_json_object(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          simdjson::ondemand::object* value);

} // namespace starrocks
