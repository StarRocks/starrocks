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
#include "storage/lake/pk_tablet_sst_writer.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/segment_writer.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

HorizontalPkTabletWriter::HorizontalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                                   std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                                   ThreadPool* flush_pool, bool is_compaction,
                                                   BundleWritableFileContext* bundle_file_context,
                                                   GlobalDictByNameMaps* global_dicts)
        : HorizontalGeneralTabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, is_compaction, flush_pool,
                                        bundle_file_context, global_dicts),
          _rowset_txn_meta(std::make_unique<RowsetTxnMetaPB>()) {
    // Note: rows_mapper_builder initialization is deferred to _init_rows_mapper_builder()
    // to support subtask_id for parallel compaction
}

HorizontalPkTabletWriter::~HorizontalPkTabletWriter() = default;

void HorizontalPkTabletWriter::_init_rows_mapper_builder() {
    if (_is_compaction && _rows_mapper_builder == nullptr && !_rows_mapper_init_attempted) {
        _rows_mapper_init_attempted = true;
        StatusOr<std::string> rows_mapper_filename;
        if (_subtask_id >= 0) {
            // Parallel compaction: use subtask-specific filename
            rows_mapper_filename = lake_rows_mapper_filename(_tablet_id, _txn_id, _subtask_id);
        } else {
            rows_mapper_filename = lake_rows_mapper_filename(_tablet_id, _txn_id);
        }
        if (rows_mapper_filename.ok()) {
            _rows_mapper_builder = std::make_unique<RowsMapperBuilder>(rows_mapper_filename.value());
        }
    }
}

Status HorizontalPkTabletWriter::write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids,
                                       SegmentPB* segment) {
    RETURN_IF_ERROR(HorizontalGeneralTabletWriter::write(data, segment));
    RETURN_IF_ERROR(_pk_sst_writer->append_sst_record(data));
    _init_rows_mapper_builder();
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->append(rssid_rowids));
    }
    return Status::OK();
}

Status HorizontalPkTabletWriter::write(const Chunk& data, SegmentPB* segment, bool eos) {
    RETURN_IF_ERROR(HorizontalGeneralTabletWriter::write(data, segment, eos));
    RETURN_IF_ERROR(_pk_sst_writer->append_sst_record(data));
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
    std::unique_ptr<WritableFile> of;
    if (_location_provider && _fs) {
        ASSIGN_OR_RETURN(of, _fs->new_writable_file(_location_provider->del_location(_tablet_id, name)));
    } else {
        ASSIGN_OR_RETURN(of, fs::new_writable_file(wopts, _tablet_mgr->del_location(_tablet_id, name)));
    }
    size_t sz = serde::ColumnArraySerde::max_serialized_size(deletes);
    std::vector<uint8_t> content(sz);
    RETURN_IF_ERROR(serde::ColumnArraySerde::serialize(deletes, content.data()));
    RETURN_IF_ERROR(of->append(Slice(content.data(), content.size())));
    RETURN_IF_ERROR(of->close());
    _files.emplace_back(FileInfo{std::move(name), content.size(), encryption_meta});
    return Status::OK();
}

Status HorizontalPkTabletWriter::reset_segment_writer(bool eos) {
    RETURN_IF_ERROR(HorizontalGeneralTabletWriter::reset_segment_writer(eos));
    // reset sst file writer
    if (_pk_sst_writer == nullptr) {
        if (enable_pk_parallel_execution()) {
            _pk_sst_writer = std::make_unique<PkTabletSSTWriter>(tablet_schema(), _tablet_mgr, _tablet_id);
        } else {
            _pk_sst_writer = std::make_unique<DefaultSSTWriter>();
        }
    }
    RETURN_IF_ERROR(_pk_sst_writer->reset_sst_writer(_location_provider, _fs));
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
        auto file_info = FileInfo{segment_name, segment_size, _seg_writer->encryption_meta()};
        if (_seg_writer->bundle_file_offset() >= 0) {
            // This is a shared data file.
            file_info.bundle_file_offset = _seg_writer->bundle_file_offset();
        }
        _files.emplace_back(file_info);
        _data_size += segment_size;
        collect_writer_stats(_stats, _seg_writer.get());
        _stats.segment_count++;
        if (segment) {
            segment->set_data_size(segment_size);
            segment->set_index_size(index_size);
            segment->set_path(segment_path);
            segment->set_encryption_meta(_seg_writer->encryption_meta());
        }
        check_global_dict(_seg_writer.get());
        _seg_writer.reset();
    }
    if (_pk_sst_writer && _pk_sst_writer->has_file_info()) {
        ASSIGN_OR_RETURN(auto sst_file_info, _pk_sst_writer->flush_sst_writer());
        _ssts.emplace_back(sst_file_info);
    }
    return Status::OK();
}

Status HorizontalPkTabletWriter::finish(SegmentPB* segment) {
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->finalize());
    }
    return HorizontalGeneralTabletWriter::finish(segment);
}

StatusOr<std::unique_ptr<TabletWriter>> HorizontalPkTabletWriter::clone() const {
    auto writer = std::make_unique<HorizontalPkTabletWriter>(_tablet_mgr, _tablet_id, _schema, _txn_id, _flush_pool,
                                                             _is_compaction, _bundle_file_context,
                                                             const_cast<GlobalDictByNameMaps*>(_global_dicts));
    RETURN_IF_ERROR(writer->open());
    if (enable_pk_parallel_execution()) {
        writer->force_set_enable_pk_parallel_execution();
    }
    writer->set_auto_flush(auto_flush());
    return writer;
}

VerticalPkTabletWriter::VerticalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                               std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                               uint32_t max_rows_per_segment, ThreadPool* flush_pool,
                                               bool is_compaction)
        : VerticalGeneralTabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, max_rows_per_segment,
                                      is_compaction, flush_pool) {
    // Note: rows_mapper_builder initialization is deferred to _init_rows_mapper_builder()
    // to support subtask_id for parallel compaction
}

VerticalPkTabletWriter::~VerticalPkTabletWriter() = default;

void VerticalPkTabletWriter::_init_rows_mapper_builder() {
    if (_is_compaction && _rows_mapper_builder == nullptr && !_rows_mapper_init_attempted) {
        _rows_mapper_init_attempted = true;
        StatusOr<std::string> rows_mapper_filename;
        if (_subtask_id >= 0) {
            // Parallel compaction: use subtask-specific filename
            rows_mapper_filename = lake_rows_mapper_filename(_tablet_id, _txn_id, _subtask_id);
        } else {
            rows_mapper_filename = lake_rows_mapper_filename(_tablet_id, _txn_id);
        }
        if (rows_mapper_filename.ok()) {
            _rows_mapper_builder = std::make_unique<RowsMapperBuilder>(rows_mapper_filename.value());
        }
    }
}

Status VerticalPkTabletWriter::write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes,
                                             bool is_key, const std::vector<uint64_t>& rssid_rowids) {
    // Save rssid_rowids only when writing key columns
    DCHECK(is_key);
    RETURN_IF_ERROR(VerticalGeneralTabletWriter::write_columns(data, column_indexes, is_key));
    if (_pk_sst_writers.size() <= _current_writer_index) {
        std::unique_ptr<DefaultSSTWriter> sst_writer;
        if (enable_pk_parallel_execution()) {
            sst_writer = std::make_unique<PkTabletSSTWriter>(tablet_schema(), _tablet_mgr, _tablet_id);
        } else {
            sst_writer = std::make_unique<DefaultSSTWriter>();
        }
        RETURN_IF_ERROR(sst_writer->reset_sst_writer(_location_provider, _fs));
        _pk_sst_writers.emplace_back(std::move(sst_writer));
    }
    RETURN_IF_ERROR(_pk_sst_writers[_current_writer_index]->append_sst_record(data));
    _init_rows_mapper_builder();
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->append(rssid_rowids));
    }
    return Status::OK();
}

Status VerticalPkTabletWriter::finish(SegmentPB* segment) {
    for (auto& sst_writer : _pk_sst_writers) {
        if (sst_writer->has_file_info()) {
            ASSIGN_OR_RETURN(auto sst_file_info, sst_writer->flush_sst_writer());
            _ssts.emplace_back(sst_file_info);
        }
    }
    _pk_sst_writers.clear();
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->finalize());
    }
    return VerticalGeneralTabletWriter::finish(segment);
}

} // namespace starrocks::lake
