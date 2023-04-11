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

#include "binary_column.h"
#include "column/column.h"
#include "common/status.h"
#include "numeric_column.h"
#include "runtime/types.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "avro.h"
#ifdef __cplusplus
}
#endif

namespace starrocks {

Status add_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                           const avro_value_t& value, bool invalid_as_null);

Status add_adaptive_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                    const avro_value_t& value, bool invalid_as_null);
} // namespace starrocks
