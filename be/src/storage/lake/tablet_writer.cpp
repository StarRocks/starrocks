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

#include "common/config_primary_key_fwd.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks::lake {

bool pk_index_eager_build_supported(const TabletSchema& schema) {
    if (schema.keys_type() != KeysType::PRIMARY_KEYS) {
        return false;
    }
    // Eager PK index build only supports the cloud-native persistent index, and shared-data primary-key
    // tables always use it (enforced since #75940/#76260). So there is no need to read tablet metadata to
    // confirm the index type -- and the eager decision no longer depends on the metadata cache being warm.
    // V2 encoding guarantees correct ordering for all column types after encoding,
    // so eager PK index build is always safe.
    if (schema.has_valid_primary_key_encoding_type() &&
        schema.primary_key_encoding_type() == PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2) {
        return true;
    }
    // For V1 encoding, a single non-VARCHAR/CHAR key column does not use big-endian encoding, which
    // may result in incorrect ordering between the sst and segment files, so eager build is unsafe.
    return schema.num_key_columns() > 1 || schema.column(0).type() == LogicalType::TYPE_VARCHAR ||
           schema.column(0).type() == LogicalType::TYPE_CHAR;
}

bool pk_preserve_txn_delete_order_enabled(const TabletSchema& schema) {
    // A separate-sort-key load's op-aware merge resolves duplicate primary keys by flush order and can
    // split a key's DELETE and later re-UPSERT across parallel merge tasks, so it MUST preserve
    // in-transaction op order (and serialize del_op_offsets) regardless of the general config -- otherwise
    // the earlier del file would erase the later re-insert.
    return config::lake_enable_pk_preserve_txn_delete_order || schema.has_separate_sort_key();
}

void TabletWriter::try_enable_pk_index_eager_build() {
    // Guard against multiple calls when writers are reused or merged in different pipeline phases
    // (e.g., eager merge and final merge); this method is intentionally idempotent.
    if (_enable_pk_index_eager_build) {
        return;
    }
    // NOTE: separate sort key is now supported via PkTabletUnsortSSTWriter, which buffers+sorts the
    // primary keys (they arrive in sort-key, not PK, order) before building the SST. It is selected
    // in the PK writer's reset path when eager build is enabled and the schema has a separate sort key.
    if (pk_index_eager_build_supported(*_schema)) {
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
    // Assign each consolidated del file an op_offset that places it right after THIS batch's segments
    // in the merged rowset. merge_other_writer() is called once per merge batch in slot order, so after
    // appending this batch's segments above, _segments.size()-1 is the global index of this batch's
    // last segment. A delete therefore (a) wins over equal keys upserted by this or earlier batches and
    // (b) loses to a re-upsert in a later batch (higher segment index) -- preserving the in-transaction
    // upsert/delete order across the parallel/batched spill merge (publish interleaves by op_offset).
    // _del_op_offsets must stay positionally aligned with _dels.
    const uint32_t del_op_offset = _segments.empty() ? 0 : static_cast<uint32_t>(_segments.size() - 1);
    for (const auto& del : other_writer->_dels) {
        _dels.push_back(del);
        _del_op_offsets.push_back(del_op_offset);
    }
    _ssts.insert(_ssts.end(), other_writer->_ssts.begin(), other_writer->_ssts.end());
    _sst_ranges.insert(_sst_ranges.end(), other_writer->_sst_ranges.begin(), other_writer->_sst_ranges.end());
    _seg_delvecs.insert(_seg_delvecs.end(), other_writer->_seg_delvecs.begin(), other_writer->_seg_delvecs.end());
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
