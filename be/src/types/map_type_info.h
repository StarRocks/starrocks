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
// limitations under the License

#pragma once

#include "storage/types.h"

namespace starrocks {

TypeInfoPtr get_map_type_info(const TypeInfoPtr& key_type, const TypeInfoPtr& value_type);
const TypeInfoPtr& get_key_type_info(const TypeInfo* type_info);
const TypeInfoPtr& get_value_type_info(const TypeInfo* type_info);

} // namespace starrocks