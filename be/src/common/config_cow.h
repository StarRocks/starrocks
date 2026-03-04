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

#include "common/configbase.h"

namespace starrocks::config {

// Enable cow optimization for column operations, used to avoid the overhead of reference counting when accessing
// columns.
CONF_mBool(enable_cow_optimization, "true");
// The diagnose level for cow optimization, 0 means no diagnose, 1 means diagnose when use_count > 1, 2 means
// diagnose when use_count > 2.
CONF_Int32(cow_optimization_diagnose_level, "0");

} // namespace starrocks::config
