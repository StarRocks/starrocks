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
#include "column/raw_data_visitor.h"
#include "column/serde/column_array_serde.h"
#include "common/config_rowset_fwd.h"
#include "common/runtime_profile.h"
#include "fs/fs_util.h"
#include "gen_cpp/Types_types.h"
#include "platform/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/pk_tablet_sst_writer.h"
#include "storage/lake/pk_tablet_unsort_sst_writer.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/segment_writer.h"
#include "storage_primitive/primary_key_encoder.h"

namespace starrocks::lake {

HorizontalPkTabletWriter::HorizontalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                                   std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                                   ThreadPool* flush_pool, bool is_compaction,
                                                   BundleWritableFileContext* bundle_file_context,
                                                   GlobalDictByNameMaps* global_dicts)
        : HorizontalGeneralTabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, is_compaction, flush_pool,
                                        bundle_file_context, global_dicts),
          _rowset_txn_meta(std::make_unique<RowsetTxnMetaPB>()) {
    if (is_compaction) {
        auto rows_mapper_filename = new_lake_rows_mapper_filename(tablet_mgr, tablet_id, txn_id);
        if (rows_mapper_filename.ok()) {
            _rows_mapper_builder = std::make_unique<RowsMapperBuilder>(rows_mapper_filename.value());
        }
    }
}

HorizontalPkTabletWriter::~HorizontalPkTabletWriter() = default;

Status HorizontalPkTabletWriter::write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids,
                                       SegmentPB* segment) {
    RETURN_IF_ERROR(HorizontalGeneralTabletWriter::write(data, segment));
    RETURN_IF_ERROR(_pk_sst_writer->append_sst_record(data, &rssid_rowids));
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

Status HorizontalPkTabletWriter::write_single_flush(const Chunk& data, SegmentPB* segment, bool eos) {
    // See TabletWriter::write_single_flush. A single flush is unique-by-PK, so it does not need the eager
    // unsort SST writer (whose sole job is cross-flush duplicate-key resolution). Clear the eager-build
    // flag so reset_segment_writer builds a DefaultSSTWriter -> no eager PK-index SST; publish then
    // rebuilds the index lazily via the order-independent parallel_upsert -- identical to the
    // eager-build-off path. Safe to clear here: a single-flush load never spills/merges/clones, so
    // reset_segment_writer (during this write) is the only remaining reader of the flag.
    _enable_pk_index_eager_build = false;
    return HorizontalGeneralTabletWriter::write(data, segment, eos);
}

Status HorizontalPkTabletWriter::write_single_flush_with_op(const Chunk& chunk_with_op, SegmentPB* segment, bool eos) {
    // See TabletWriter::write_single_flush_with_op. The memtable already deduplicated this flush by
    // primary key (last-op-wins), so it is unique-by-PK and each key is EITHER an upsert OR a delete --
    // the unsort SST writer's cross-flush dedup is unnecessary. Split __op and route each part through the
    // single-flush write: upserts to a plain sort-key-ordered segment (eager SST skipped), deletes encoded
    // to a del file. Mirrors MemTable::_split_upserts_deletes + the flush_chunk_with_deletes shortcut.

    // Work on a mutable copy: RawDataVisitor reads the op column via mutable_raw_data(), and we drop the
    // __op column before writing the segment.
    auto data_chunk = chunk_with_op.clone_unique();
    const size_t op_col_id = data_chunk->num_columns() - 1;
    const size_t nrows = data_chunk->num_rows();

    // Read the trailing __op column (0 == UPSERT, otherwise DELETE) and partition rows BEFORE removing it
    // (the raw-data pointer is invalidated by remove_column_by_index). Same as MemTable::_split_upserts_deletes.
    RawDataVisitor visitor;
    RETURN_IF_ERROR(data_chunk->get_column_by_index(op_col_id)->accept(&visitor));
    const auto* ops = visitor.result();
    std::vector<uint32_t> upsert_idx;
    std::vector<uint32_t> delete_idx;
    for (uint32_t i = 0; i < nrows; i++) {
        (ops[i] == TOpType::DELETE ? delete_idx : upsert_idx).push_back(i);
    }
    const size_t ndel = delete_idx.size();

    // Drop __op so the chunk matches the segment schema (data columns only).
    data_chunk->remove_column_by_index(op_col_id);

    if (ndel == 0) {
        // All upserts: write the whole flush as one segment.
        return write_single_flush(*data_chunk, segment, eos);
    }

    // Encode the delete rows' primary keys to a del file (same encoding the primary index uses).
    ASSIGN_OR_RETURN(auto pk_encoding_type, tablet_schema()->primary_key_encoding_type_or_error());
    std::vector<uint32_t> pk_columns;
    pk_columns.reserve(tablet_schema()->num_key_columns());
    for (size_t i = 0; i < tablet_schema()->num_key_columns(); i++) {
        pk_columns.push_back(static_cast<uint32_t>(i));
    }
    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema(), pk_columns);
    MutableColumnPtr deletes;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &deletes, pk_encoding_type));
    PrimaryKeyEncoder::encode_selective(pkey_schema, *data_chunk, delete_idx.data(), delete_idx.size(), deletes.get(),
                                        pk_encoding_type);
    RETURN_IF_ERROR(flush_del_file(*deletes, kUnknownDelOpOffset));

    // Write only the upsert rows to the segment.
    const size_t nupsert = upsert_idx.size();
    auto upserts = data_chunk->clone_empty_with_schema(nupsert);
    upserts->append_selective(*data_chunk, upsert_idx.data(), 0, nupsert);
    return write_single_flush(*upserts, segment, eos);
}

Status HorizontalPkTabletWriter::append_pk_index_deletes(const Chunk& data, const std::vector<uint64_t>& rssid_rowids) {
    // The op-aware merge can feed DELETE rows before the first write() that lazily sets up the segment
    // and SST writers (a delete-first or delete-only batch), so _pk_sst_writer may still be null here.
    // Do the same lazy init write() does; guarding on _seg_writer == nullptr means the following upsert
    // write() reuses this same segment/SST writer rather than resetting it (which would drop the deletes).
    if (_seg_writer == nullptr) {
        RETURN_IF_ERROR(reset_segment_writer(false));
    }
    // DELETE rows are only fed to the eager PK-index SST writer (to resolve the latest op per key);
    // they are NOT written to a segment. Only the unsort SST writer acts on them; other SST writers
    // no-op (deletes reach them via the caller's del file instead).
    return _pk_sst_writer->append_delete_records(data, &rssid_rowids);
}

Status HorizontalPkTabletWriter::flush_del_file(const Column& deletes, uint32_t op_offset) {
    auto name = gen_del_filename(_txn_id);
    WritableFileOptions wopts;
    std::string encryption_meta;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        encryption_meta.swap(pair.encryption_meta);
    }
    RETURN_IF_ERROR(PrimaryKeyEncoder::check_delete_file_binary_column_size(deletes));
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
    {
        // Use _dels_mutex to protect _dels concurrenctly append by multiple threads.
        std::lock_guard lg(_dels_mutex);
        _dels.emplace_back(FileInfo{std::move(name), content.size(), encryption_meta});
        // Keep _del_op_offsets positionally aligned with _dels.
        _del_op_offsets.emplace_back(op_offset);
    }
    return Status::OK();
}

Status HorizontalPkTabletWriter::reset_segment_writer(bool eos) {
    RETURN_IF_ERROR(HorizontalGeneralTabletWriter::reset_segment_writer(eos));
    // reset sst file writer
    if (_pk_sst_writer == nullptr) {
        if (enable_pk_index_eager_build()) {
            if (tablet_schema()->has_separate_sort_key()) {
                // Separate sort key: primary keys arrive in sort-key (not PK) order, so buffer+sort
                // them in the unsort writer instead of streaming into a sorted SST.
                _pk_sst_writer = std::make_unique<PkTabletUnsortSSTWriter>(tablet_schema(), _tablet_mgr, _tablet_id);
            } else {
                _pk_sst_writer = std::make_unique<PkTabletSSTWriter>(tablet_schema(), _tablet_mgr, _tablet_id);
            }
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
        SegmentFileInfo& segment_file_info = _segments.emplace_back();
        const std::string& segment_path = _seg_writer->segment_path();
        segment_file_info.path = std::string(basename(segment_path));
        segment_file_info.size = segment_size;
        segment_file_info.encryption_meta = _seg_writer->encryption_meta();
        if (_seg_writer->bundle_file_offset() >= 0) {
            // This is a shared data file.
            segment_file_info.bundle_file_offset = _seg_writer->bundle_file_offset();
        }
        _seg_writer->write_sort_key_fields_to(segment_file_info);
        segment_file_info.num_rows = _seg_writer->num_rows();
        // Mirror the duplicate-key path: without this, shared-data primary-key tables never
        // record vector_index_ids, so async builds are never scheduled and sync-built .vi
        // files are never vacuumed.
        record_segment_vector_index_ids(segment_file_info, _seg_writer.get());
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
        ASSIGN_OR_RETURN(auto sst_ret, _pk_sst_writer->flush_sst_writer());
        auto [sst_file_info, sst_range] = std::move(sst_ret);
        _ssts.emplace_back(sst_file_info);
        _sst_ranges.emplace_back(sst_range);
        // Keep _seg_delvecs positionally aligned with _ssts (empty for the sort-key==PK path).
        _seg_delvecs.emplace_back(_pk_sst_writer->take_deleted_rowids());
        // The unsort writer (separate-sort-key op-aware path) cannot route deletes through the merge
        // task's del file, so it hands back the winning DELETEs' encoded keys here; write them to a del
        // file so publish erases the rows (including any written by an earlier transaction). op_offset is
        // kUnknownDelOpOffset -- the real value is assigned when this batch is consolidated (merge_other_writer).
        if (auto del_keys = _pk_sst_writer->take_delete_keys(); del_keys != nullptr && !del_keys->empty()) {
            RETURN_IF_ERROR(flush_del_file(*del_keys, kUnknownDelOpOffset));
        }
    }
    return Status::OK();
}

Status HorizontalPkTabletWriter::finish(SegmentPB* segment) {
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->finalize());
        _lcrm_file = _rows_mapper_builder->file_info();
    }
    return HorizontalGeneralTabletWriter::finish(segment);
}

StatusOr<std::unique_ptr<TabletWriter>> HorizontalPkTabletWriter::clone() const {
    auto writer = std::make_unique<HorizontalPkTabletWriter>(_tablet_mgr, _tablet_id, _schema, _txn_id, _flush_pool,
                                                             _is_compaction, _bundle_file_context,
                                                             const_cast<GlobalDictByNameMaps*>(_global_dicts));
    RETURN_IF_ERROR(writer->open());
    if (enable_pk_index_eager_build()) {
        writer->force_set_enable_pk_index_eager_build();
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
    if (is_compaction) {
        auto rows_mapper_filename = new_lake_rows_mapper_filename(tablet_mgr, tablet_id, txn_id);
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
    if (_pk_sst_writers.size() <= _current_writer_index) {
        std::unique_ptr<DefaultSSTWriter> sst_writer;
        if (enable_pk_index_eager_build()) {
            if (tablet_schema()->has_separate_sort_key()) {
                // Separate sort key: the vertical compaction task moves the PK columns into the key
                // column group (appended after the sort-key columns), so this writer sees them here
                // but not at chunk positions [0, num_key). The unsort writer buffers+sorts the PKs
                // (they arrive in sort-key order) and projects them via `column_indexes`.
                sst_writer = std::make_unique<PkTabletUnsortSSTWriter>(tablet_schema(), _tablet_mgr, _tablet_id);
            } else {
                sst_writer = std::make_unique<PkTabletSSTWriter>(tablet_schema(), _tablet_mgr, _tablet_id);
            }
        } else {
            sst_writer = std::make_unique<DefaultSSTWriter>();
        }
        RETURN_IF_ERROR(sst_writer->reset_sst_writer(_location_provider, _fs));
        _pk_sst_writers.emplace_back(std::move(sst_writer));
    }
    RETURN_IF_ERROR(_pk_sst_writers[_current_writer_index]->append_sst_record(data, &rssid_rowids, &column_indexes));
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->append(rssid_rowids));
    }
    return Status::OK();
}

Status VerticalPkTabletWriter::finish(SegmentPB* segment) {
    for (auto& sst_writer : _pk_sst_writers) {
        if (sst_writer->has_file_info()) {
            ASSIGN_OR_RETURN(auto sst_ret, sst_writer->flush_sst_writer());
            auto [sst_file_info, sst_range] = std::move(sst_ret);
            _ssts.emplace_back(sst_file_info);
            _sst_ranges.emplace_back(sst_range);
            _seg_delvecs.emplace_back(sst_writer->take_deleted_rowids());
        }
    }
    _pk_sst_writers.clear();
    if (_rows_mapper_builder != nullptr) {
        RETURN_IF_ERROR(_rows_mapper_builder->finalize());
        _lcrm_file = _rows_mapper_builder->file_info();
    }
    return VerticalGeneralTabletWriter::finish(segment);
}

} // namespace starrocks::lake
