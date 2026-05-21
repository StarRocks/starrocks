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

// PoC forward declarations for the lake PK secondary index configs.
// Mirrors the canonical declarations in be/src/common/config.h so that
// translation units in the secondary_sorted module and the three hook
// sites (delta_writer / horizontal_compaction / vertical_compaction /
// tablet_reader) can pull just these symbols without dragging in the
// full config.h.

#pragma once

#include "common/configbase.h"

namespace starrocks::config {

// Enable building secondary index files during load and compaction.
CONF_mBool(enable_secondary_index_write, "false");
// Enable using secondary index files during query.
CONF_mBool(enable_secondary_index_read, "false");
// Memory limit (in MB) for sorting (idx_cols, seg_id, rowid) entries.
CONF_mInt64(secondary_index_build_mem_limit_mb, "512");
// PoC index registry. Format:
//   "tablet_id:index_name:col1,col2;tablet_id:index_name:col"
CONF_mString(poc_secondary_index_defs, "");

} // namespace starrocks::config
