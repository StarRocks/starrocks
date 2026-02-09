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
#include "storage/rowset/segment_writer.h"

namespace starrocks::lake {

void TabletWriter::try_enable_pk_index_eager_build() {
    // Guard against multiple calls when writers are reused or merged in different pipeline phases
    // (e.g., eager merge and final merge); this method is intentionally idempotent.
    if (_enable_pk_index_eager_build) {
        return;
    }
    if (!config::enable_pk_index_eager_build || _schema->keys_type() != KeysType::PRIMARY_KEYS ||
        _schema->has_separate_sort_key()) {
        return;
    }
    auto metadata = _tablet_mgr->get_latest_cached_tablet_metadata(_tablet_id);
    if (metadata != nullptr) {
        // Eager PK index build only supports cloud native pk index.
        if (!metadata->enable_persistent_index() ||
            metadata->persistent_index_type() != PersistentIndexTypePB::CLOUD_NATIVE) {
            return;
        }
    }
    // For primary key table with single key column and the type is not VARCHAR/CHAR,
    // we can't enable eager PK index build. The reason is that, in the current implementation,
    // when encoding a single-key column of a non-binary type, big-endian encoding is not used,
    // which may result in incorrect ordering between sst and segment files.
    // This is a legacy bug, but for compatibility reasons, it will not be supported in the first phase.
    // Will fix it later.
    if (_schema->num_key_columns() > 1 || _schema->column(0).type() == LogicalType::TYPE_VARCHAR ||
        _schema->column(0).type() == LogicalType::TYPE_CHAR) {
        _enable_pk_index_eager_build = true;
    }
}

void TabletWriter::check_global_dict(SegmentWriter* segment_writer) {
    const auto& seg_global_dict_columns_valid_info = segment_writer->global_dict_columns_valid_info();
    for (const auto& it : seg_global_dict_columns_valid_info) {
        if (!it.second) {
            _global_dict_columns_valid_info[it.first] = false;
        } else {
            if (const auto& iter = _global_dict_columns_valid_info.find(it.first);
                iter == _global_dict_columns_valid_info.end()) {
                _global_dict_columns_valid_info[it.first] = true;
            }
        }
    }
}

Status TabletWriter::merge_other_writers(const std::vector<std::unique_ptr<TabletWriter>>& other_writers) {
    // merge other writers' files into current writer
    for (const auto& writer : other_writers) {
        RETURN_IF_ERROR(merge_other_writer(writer.get()));
    }
    return Status::OK();
}

Status TabletWriter::merge_other_writer(const TabletWriter* other_writer) {
    // merge other writer's files into current writer
    _segments.insert(_segments.end(), other_writer->_segments.begin(), other_writer->_segments.end());
    _dels.insert(_dels.end(), other_writer->_dels.begin(), other_writer->_dels.end());
    _ssts.insert(_ssts.end(), other_writer->_ssts.begin(), other_writer->_ssts.end());
    _sst_ranges.insert(_sst_ranges.end(), other_writer->_sst_ranges.begin(), other_writer->_sst_ranges.end());
    _num_rows += other_writer->_num_rows;
    _data_size += other_writer->_data_size;
    // _global_dict_columns_valid_info
    for (const auto& [col, valid] : other_writer->_global_dict_columns_valid_info) {
        if (!valid) {
            _global_dict_columns_valid_info[col] = false;
        } else if (_global_dict_columns_valid_info.find(col) == _global_dict_columns_valid_info.end()) {
            _global_dict_columns_valid_info[col] = true;
        }
    }
    return Status::OK();
}

} // namespace starrocks::lake