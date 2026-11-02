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

#include <unordered_map>
#include <unordered_set>

#include "base/status.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks::lake {

// Only for the opt-in, scalar add/drop path. Every known physical column is either
// retained in the UID map or explicitly dropped. Keep identity mappings so strict
// footer validation remains enabled even when source and target UIDs already match.
Status build_light_schema_change_uid_map(const TabletMetadataPB& source, const TabletMetadataPB& target,
                                         std::unordered_map<uint32_t, uint32_t>* uid_map,
                                         std::unordered_set<uint32_t>* dropped_column_uids);
void normalize_replication_rowset_schemas(TabletMetadataPB* metadata);
// Run BEFORE mutating destination metadata. Replay must not depend on a mutable config.
Status validate_normalized_replication_schema(const TxnLogPB::OpReplication& op, const TabletMetadataPB& target);

} // namespace starrocks::lake
