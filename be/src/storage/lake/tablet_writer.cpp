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

#include "storage/lake/tablet_writer.h"

#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

void TabletWriter::try_enable_pk_parallel_execution() {
    if (!config::enable_pk_parallel_execution || _schema->keys_type() != KeysType::PRIMARY_KEYS ||
        _schema->has_separate_sort_key()) {
        return;
    }
    auto metadata = _tablet_mgr->get_latest_cached_tablet_metadata(_tablet_id);
    if (metadata != nullptr) {
        // Pk parallel execution only support cloud native pk index.
        if (!metadata->enable_persistent_index() ||
            metadata->persistent_index_type() != PersistentIndexTypePB::CLOUD_NATIVE) {
            return;
        }
    }
    // For primary key table with single key column and the type is not VARCHAR/CHAR,
    // we can't enable pk parrallel execution. The reason is that, in the current implementation,
    // when encoding a single-key column of a non-binary type, big-endian encoding is not used,
    // which may result in incorrect ordering between sst and segment files.
    // This is a legacy bug, but for compatibility reasons, it will not be supported in the first phase.
    // Will fix it later.
    if (_schema->num_key_columns() > 1 || _schema->column(0).type() == LogicalType::TYPE_VARCHAR ||
        _schema->column(0).type() == LogicalType::TYPE_CHAR) {
        _enable_pk_parallel_execution = true;
    }
}

} // namespace starrocks::lake