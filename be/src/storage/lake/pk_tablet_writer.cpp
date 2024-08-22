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

#include "storage/lake/pk_tablet_writer.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "runtime/exec_env.h"
#include "serde/column_array_serde.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/segment_writer.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

HorizontalPkTabletWriter::HorizontalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                                   std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                                   ThreadPool* flush_pool, bool is_compaction)
        : HorizontalGeneralTabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, is_compaction, flush_pool),
          _rowset_txn_meta(std::make_unique<RowsetTxnMetaPB>()) {
    if (is_compaction) {
        auto rows_mapper_filename = lake_rows_mapper_filename(tablet_id, txn_id);
        if (rows_mapper_filename.ok()) {
            _rows_mapper_builder = std::make_unique<RowsMapperBuilder>(rows_mapper_filename.value());
        }
    }
}

HorizontalPkTabletWriter::~HorizontalPkTabletWriter() = default;

Status HorizontalPkTabletWriter::write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids,
                                       SegmentPB* segment) {
    RETURN_IF_ERROR(HorizontalGeneralTabletWriter::write(data, segment));
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->append(rssid_rowids));
    }
    return Status::OK();
}

Status HorizontalPkTabletWriter::flush_del_file(const Column& deletes) {
    auto name = gen_del_filename(_txn_id);
    WritableFileOptions wopts;
    std::string encryption_meta;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        encryption_meta.swap(pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto of, fs::new_writable_file(wopts, _tablet_mgr->del_location(_tablet_id, name)));
    size_t sz = serde::ColumnArraySerde::max_serialized_size(deletes);
    std::vector<uint8_t> content(sz);
    if (serde::ColumnArraySerde::serialize(deletes, content.data()) == nullptr) {
        return Status::InternalError("deletes column serialize failed");
    }
    RETURN_IF_ERROR(of->append(Slice(content.data(), content.size())));
    RETURN_IF_ERROR(of->close());
    _files.emplace_back(FileInfo{std::move(name), content.size(), encryption_meta});
    return Status::OK();
}

Status HorizontalPkTabletWriter::flush_segment_writer(SegmentPB* segment) {
    if (_seg_writer != nullptr) {
        uint64_t segment_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(_seg_writer->finalize(&segment_size, &index_size, &footer_position));
        // partial update
        auto* partial_rowset_footer = _rowset_txn_meta->add_partial_rowset_footers();
        partial_rowset_footer->set_position(footer_position);
        partial_rowset_footer->set_size(segment_size - footer_position);
        const std::string& segment_path = _seg_writer->segment_path();
        std::string segment_name = std::string(basename(segment_path));
        _files.emplace_back(FileInfo{segment_name, segment_size, _seg_writer->encryption_meta()});
        _data_size += segment_size;
        if (segment) {
            segment->set_data_size(segment_size);
            segment->set_index_size(index_size);
            segment->set_path(segment_path);
            segment->set_encryption_meta(_seg_writer->encryption_meta());
        }
        _seg_writer.reset();
    }
    return Status::OK();
}

Status HorizontalPkTabletWriter::finish(SegmentPB* segment) {
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->finalize());
    }
    return HorizontalGeneralTabletWriter::finish(segment);
}

VerticalPkTabletWriter::VerticalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                               std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                               uint32_t max_rows_per_segment, ThreadPool* flush_pool,
                                               bool is_compaction)
        : VerticalGeneralTabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, max_rows_per_segment,
                                      is_compaction, flush_pool) {
    if (is_compaction) {
        auto rows_mapper_filename = lake_rows_mapper_filename(tablet_id, txn_id);
        if (rows_mapper_filename.ok()) {
            _rows_mapper_builder = std::make_unique<RowsMapperBuilder>(rows_mapper_filename.value());
        }
    }
}

VerticalPkTabletWriter::~VerticalPkTabletWriter() = default;

Status VerticalPkTabletWriter::write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes,
                                             bool is_key, const std::vector<uint64_t>& rssid_rowids) {
    // Save rssid_rowids only when writing key columns
    DCHECK(is_key);
    RETURN_IF_ERROR(VerticalGeneralTabletWriter::write_columns(data, column_indexes, is_key));
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->append(rssid_rowids));
    }
    return Status::OK();
}

Status VerticalPkTabletWriter::finish(SegmentPB* segment) {
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->finalize());
    }
    return VerticalGeneralTabletWriter::finish(segment);
}

} // namespace starrocks::lake
