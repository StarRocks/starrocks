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

#include "connector/iceberg/iceberg_dv_sink.h"

#include <fmt/format.h>

#include <set>

#include "base/uid_util.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "connector/common/partition_chunk_writer_memory_manager.h"
#include "connector_primitive/sink_memory_manager.h"
#include "formats/column_evaluator.h"
#include "formats/iceberg/iceberg_delete_builder.h"
#include "formats/iceberg/iceberg_deletion_vector_reader.h"
#include "formats/io/async_flush_stream_poller.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

IcebergDvSinkProvider::IcebergDvSinkProvider(std::shared_ptr<IcebergDvSinkContext> ctx) : _ctx(std::move(ctx)) {}

StatusOr<std::unique_ptr<ConnectorSink>> IcebergDvSinkProvider::create_sink(int32_t driver_id) {
    auto ctx = _ctx;
    if (ctx == nullptr) {
        return Status::InternalError("IcebergDvSinkProvider: context is not IcebergDvSinkContext");
    }
    if (ctx->column_slot_map.find("_file") == ctx->column_slot_map.end()) {
        return Status::InternalError("Could not find _file column in column_slot_map");
    }
    if (ctx->column_slot_map.find("_pos") == ctx->column_slot_map.end()) {
        return Status::InternalError("Could not find _pos column in column_slot_map");
    }

    auto* runtime_state = ctx->runtime_state;
    DCHECK(runtime_state != nullptr);
    ASSIGN_OR_RETURN(auto fs,
                     FileSystemFactory::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_configuration)));
    std::shared_ptr<FileSystem> shared_fs = std::move(fs);

    // Puffin DV lives at the table data root (no partition subdir): suffix "puffin".
    auto location_provider =
            std::make_shared<LocationProvider>(ctx->path, print_id(runtime_state->query_id()),
                                               runtime_state->be_number(), driver_id, "puffin", ctx->writer_tag);

    return std::make_unique<IcebergDvSink>(shared_fs, location_provider, ctx->column_slot_map,
                                           ctx->previous_delete_files, runtime_state);
}

IcebergDvSink::IcebergDvSink(std::shared_ptr<FileSystem> fs, std::shared_ptr<LocationProvider> location_provider,
                             std::unordered_map<std::string, TExprNode> column_slot_map,
                             std::shared_ptr<const std::vector<TIcebergPreviousDeleteFile>> previous_delete_files,
                             RuntimeState* state)
        : PartitionedConnectorChunkSink(/*partition_columns=*/{}, /*partition_column_evaluators=*/{},
                                        /*partition_chunk_writer_factory=*/nullptr, state,
                                        /*support_null_partition=*/true),
          _fs(std::move(fs)),
          _location_provider(std::move(location_provider)),
          _column_slot_map(std::move(column_slot_map)),
          _previous_delete_files(std::move(previous_delete_files)) {}

Status IcebergDvSink::init(formats::AsyncFlushStreamPoller* poller, RuntimeProfile* profile,
                           SinkMemoryManager* sink_mem_mgr) {
    // Mirror PartitionedConnectorChunkSink::init, minus the PartitionChunkWriterFactory init:
    // the DV sink owns no partition writers (factory is null) and finalizes a single Puffin at
    // finish(). The op_mem_mgr wiring is still required: SinkMemoryManager polls the child
    // manager's releasable_memory() -> _io_poller on every need_input(), so _io_poller and the
    // child manager must be set here or that path null-derefs. With an empty _writers list the
    // memory manager applies no backpressure (DV bitmaps cannot be flushed incrementally).
    _io_poller = poller;
    _profile = profile;
    DCHECK(sink_mem_mgr != nullptr);
    auto op_mem_mgr = std::make_unique<PartitionChunkWriterMemoryManager>();
    init_profile();
    RETURN_IF_ERROR(op_mem_mgr->init(&_writers, _io_poller));
    _partition_writer_mem_mgr = op_mem_mgr.get();
    _op_mem_mgr = sink_mem_mgr->register_child_manager(std::move(op_mem_mgr));
    return Status::OK();
}

Status IcebergDvSink::add(const ChunkPtr& chunk) {
    const int num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    auto file_it = _column_slot_map.find("_file");
    auto pos_it = _column_slot_map.find("_pos");
    if (file_it == _column_slot_map.end() || pos_it == _column_slot_map.end()) {
        return Status::InternalError("Could not find _file/_pos in column_slot_map");
    }
    const SlotId file_slot = file_it->second.slot_ref.slot_id;
    const SlotId pos_slot = pos_it->second.slot_ref.slot_id;

    ColumnPtr file_col = chunk->get_column_by_slot_id(file_slot);
    ColumnPtr pos_col = chunk->get_column_by_slot_id(pos_slot);
    if (file_col == nullptr || pos_col == nullptr) {
        return Status::InternalError("Could not find _file/_pos column in chunk");
    }
    BinaryColumn* file_data = ColumnHelper::get_binary_column(chunk->get_column_raw_ptr_by_slot_id(file_slot));
    ColumnViewer<TYPE_BIGINT> pos_view(pos_col);

    // The sink is partition-agnostic: the FE commit derives each entry's partition from the
    // referenced data file itself.
    for (int i = 0; i < num_rows; ++i) {
        if (file_col->is_null(i)) {
            return Status::InternalError("_file is NULL value");
        }
        if (pos_col->is_null(i)) {
            return Status::InternalError("_pos is NULL value");
        }
        _dv_writer.add(file_data->get_slice(i), static_cast<uint64_t>(pos_view.value(i)));
    }
    return Status::OK();
}

Status IcebergDvSink::read_previous_delete_rows(const TIcebergPreviousDeleteFile& prev,
                                                const formats::IcebergPositionDeleteReader::RowCallback& cb) const {
    // Prefer the FE-forwarded length: get_file_size is unsupported on object storage.
    int64_t length;
    if (prev.__isset.file_size_in_bytes) {
        length = prev.file_size_in_bytes;
    } else {
        ASSIGN_OR_RETURN(length, _fs->get_file_size(prev.path));
    }
    ASSIGN_OR_RETURN(auto file, _fs->new_random_access_file(prev.path));
    return formats::IcebergPositionDeleteReader::read_rows(file.get(), prev.path, length, prev.format,
                                                           _state->chunk_size(), _state->timezone(),
                                                           FormatScannerOptions{}, /*stats=*/nullptr, cb);
}

Status IcebergDvSink::merge_previous_deletes(
        std::map<std::string, std::vector<TIcebergPreviousDeleteFile>, std::less<>>* rewritten_by_ref) {
    if (_previous_delete_files == nullptr) {
        return Status::OK();
    }
    for (const auto& prev : *_previous_delete_files) {
        if (!prev.__isset.referenced_data_files || prev.referenced_data_files.empty()) {
            return Status::InternalError("previous delete file is missing referenced_data_files: " + prev.path);
        }
        const bool file_scoped = prev.__isset.file_scoped && prev.file_scoped;
        if (!file_scoped) {
            // Partition-scoped position delete: merged for this driver's touched files, but NOT
            // reported as rewritten — other data files in the partition still rely on it.
            std::set<std::string_view> touched_refs;
            for (const auto& ref : prev.referenced_data_files) {
                if (_dv_writer.contains(ref)) {
                    touched_refs.insert(ref);
                }
            }
            if (touched_refs.empty()) {
                continue;
            }
            RETURN_IF_ERROR(read_previous_delete_rows(prev, [&](const Slice& file_path, int64_t pos) {
                if (touched_refs.find(std::string_view(file_path)) != touched_refs.end()) {
                    _dv_writer.add(file_path, static_cast<uint64_t>(pos));
                }
            }));
            continue;
        }
        if (prev.referenced_data_files.size() != 1) {
            return Status::InternalError("file-scoped previous delete must reference exactly one data file: " +
                                         prev.path);
        }
        const std::string& ref = prev.referenced_data_files[0];
        if (!_dv_writer.contains(ref)) {
            continue; // untouched data file: no new DV is written for it, leave it alone
        }
        if (prev.format == "puffin") {
            if (!prev.__isset.content_offset || !prev.__isset.content_size_in_bytes) {
                return Status::InternalError(
                        "previous deletion vector is missing content_offset/content_size_in_bytes: " + prev.path);
            }
            ASSIGN_OR_RETURN(auto file, _fs->new_random_access_file(prev.path));
            std::vector<uint8_t> buffer(prev.content_size_in_bytes);
            RETURN_IF_ERROR(file->read_at_fully(prev.content_offset, buffer.data(), buffer.size()));
            ASSIGN_OR_RETURN(auto* bm, formats::IcebergDeletionVectorReader::parse_dv_blob(
                                               buffer.data(), buffer.size(),
                                               prev.__isset.record_count ? prev.record_count : -1, nullptr));
            _dv_writer.merge_bitmap(ref, bm);
            roaring64_bitmap_free(bm);
        } else {
            // A file-scoped delete references exactly one data file; the filter guards
            // malformed rows.
            RETURN_IF_ERROR(read_previous_delete_rows(prev, [&](const Slice& file_path, int64_t pos) {
                if (file_path == ref) {
                    _dv_writer.add(file_path, static_cast<uint64_t>(pos));
                }
            }));
        }
        (*rewritten_by_ref)[ref].push_back(prev);
    }
    return Status::OK();
}

Status IcebergDvSink::finish() {
    _finished = true;
    if (_dv_writer.empty()) {
        return Status::OK(); // nothing deleted -> no orphan zero-blob Puffin
    }

    // Snapshot per-file counts BEFORE merging: the metrics must report this DELETE's rows,
    // not the merged DV cardinality (which includes historical positions).
    const auto added_rows_by_file = _dv_writer.file_cardinalities();
    int64_t current_delete_rows = 0;
    for (const auto& [_, rows] : added_rows_by_file) {
        current_delete_rows += rows;
    }

    std::map<std::string, std::vector<TIcebergPreviousDeleteFile>, std::less<>> rewritten_by_ref;
    RETURN_IF_ERROR(merge_previous_deletes(&rewritten_by_ref));

    const std::string location = _location_provider->get(); // <data_root>/{prefix}_{idx}.puffin
    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(location));
    // Register cleanup right after the file exists: if the query is cancelled after finish()
    // but before the FE commit (or the write below fails), ConnectorSinkOperator::close()
    // invokes rollback() and this deletes the uncommitted Puffin, matching the Parquet sinks.
    push_rollback_action([fs = _fs, location]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(location)), "fail to delete file " + location);
    });
    ASSIGN_OR_RETURN(std::vector<formats::IcebergDvCommitEntry> entries, _dv_writer.finish(file.get()));
    // Total Puffin size including the footer PuffinWriter appended after the last blob; the
    // last blob's end offset would understate it. Read before close() while the file is open.
    const int64_t file_size = static_cast<int64_t>(file->size());
    RETURN_IF_ERROR(file->close());

    for (const auto& e : entries) {
        TIcebergDataFile df;
        df.__set_path(location);
        df.__set_format("puffin");
        df.__set_file_content(TIcebergFileContent::POSITION_DELETES);
        df.__set_referenced_data_file(e.referenced_data_file);
        df.__set_content_offset(e.content_offset);
        df.__set_content_size_in_bytes(e.content_size_in_bytes);
        df.__set_record_count(e.record_count);
        df.__set_file_size_in_bytes(file_size);
        if (auto it = added_rows_by_file.find(e.referenced_data_file); it != added_rows_by_file.end()) {
            df.__set_added_delete_rows(it->second);
        }
        TSinkCommitInfo commit_info;
        commit_info.__set_iceberg_data_file(df);
        // The FE removes exactly the file-scoped deletes folded into this file's new DV.
        auto rit = rewritten_by_ref.find(e.referenced_data_file);
        if (rit != rewritten_by_ref.end()) {
            commit_info.__set_rewritten_delete_files(rit->second);
        }
        _state->add_sink_commit_info(commit_info);
    }
    _state->update_num_rows_load_sink(current_delete_rows);
    return Status::OK();
}

} // namespace starrocks::connector
