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

#include "storage/flat_json_config.h"

#include "common/config.h"

namespace starrocks {

FlatJsonConfig::FlatJsonConfig()
        : _flat_json_null_factor(config::json_flat_null_factor),
          _flat_json_sparsity_factor(config::json_flat_sparsity_factor),
          _flat_json_max_column_max(config::json_flat_column_max) {}

} // namespace starrocks
