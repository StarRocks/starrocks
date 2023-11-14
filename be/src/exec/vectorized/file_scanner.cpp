// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/file_scanner.h"

#include <memory>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "fs/fs.h"
#include "fs/fs_broker.h"
#include "fs/fs_hdfs.h"
#include "gutil/strings/substitute.h"
#include "io/compressed_input_stream.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "util/compression/stream_compression.h"

namespace starrocks::vectorized {

FileScanner::FileScanner(starrocks::RuntimeState* state, starrocks::RuntimeProfile* profile,
                         const starrocks::TBrokerScanRangeParams& params,
                         starrocks::vectorized::ScannerCounter* counter)
        : _state(state),
          _profile(profile),
          _params(params),
          _counter(counter),
          _row_desc(nullptr),
          _strict_mode(false),
          _error_counter(0) {}

FileScanner::~FileScanner() = default;

void FileScanner::close() {
    Expr::close(_dest_expr_ctx, _state);
}

Status FileScanner::init_expr_ctx() {
    const TupleDescriptor* src_tuple_desc = _state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);

    if (src_tuple_desc == nullptr) {
        return Status::InternalError(
                strings::Substitute("Unknown source tuple descriptor, tuple_id=$0", _params.src_tuple_id));
    }

    // sources
    std::unordered_map<SlotId, SlotDescriptor*> src_slot_desc_map;
    for (const auto& slot_desc : src_tuple_desc->slots()) {
        src_slot_desc_map.emplace(slot_desc->id(), slot_desc);
    }

    for (auto slot_id : _params.src_slot_ids) {
        auto it = src_slot_desc_map.find(slot_id);
        if (it == std::end(src_slot_desc_map)) {
            _src_slot_descriptors.emplace_back(nullptr);
            continue;
        }

        _src_slot_descriptors.emplace_back(it->second);
    }

    _row_desc = std::make_unique<RowDescriptor>(_state->desc_tbl(), std::vector<TupleId>{_params.src_tuple_id},
                                                std::vector<bool>{false});

    // destination
    _dest_tuple_desc = _state->desc_tbl().get_tuple_descriptor(_params.dest_tuple_id);
    if (_dest_tuple_desc == nullptr) {
        return Status::InternalError(
                strings::Substitute("Unknown dest tuple descriptor, tuple_id=$0", _params.dest_tuple_id));
    }

    bool has_slot_id_map = _params.__isset.dest_sid_to_src_sid_without_trans;

    for (const SlotDescriptor* slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

        auto it = _params.expr_of_dest_slot.find(slot_desc->id());
        if (it == std::end(_params.expr_of_dest_slot)) {
            return Status::InternalError(strings::Substitute("No expr for dest slot, id=$0, name=$1", slot_desc->id(),
                                                             slot_desc->col_name()));
        }

        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
        RETURN_IF_ERROR(ctx->prepare(_state));
        RETURN_IF_ERROR(ctx->open(_state));

        _dest_expr_ctx.emplace_back(ctx);

        if (has_slot_id_map) {
            auto it = _params.dest_sid_to_src_sid_without_trans.find(slot_desc->id());

            if (it == std::end(_params.dest_sid_to_src_sid_without_trans)) {
                _dest_slot_desc_mappings.emplace_back(nullptr);
            } else {
                auto _src_slot_it = src_slot_desc_map.find(it->second);
                if (_src_slot_it == std::end(src_slot_desc_map)) {
                    return Status::InternalError(strings::Substitute("No src slot $0 in src slot desc", it->second));
                }
                _dest_slot_desc_mappings.emplace_back(_src_slot_it->second);
            }
        }
    }
    return Status::OK();
}

Status FileScanner::open() {
    RETURN_IF_ERROR(init_expr_ctx());

    if (_params.__isset.strict_mode) {
        _strict_mode = _params.strict_mode;
    }

    if (_strict_mode && !_params.__isset.dest_sid_to_src_sid_without_trans) {
        return Status::InternalError("Slot map of dest to src must be set in strict mode");
    }
    return Status::OK();
}

void FileScanner::fill_columns_from_path(starrocks::vectorized::ChunkPtr& chunk, int slot_start,
                                         const std::vector<std::string>& columns_from_path, int size) {
    auto varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    // fill column with partition values.
    for (int i = 0; i < columns_from_path.size(); ++i) {
        auto slot_desc = _src_slot_descriptors.at(i + slot_start);
        if (slot_desc == nullptr) continue;
        auto col = ColumnHelper::create_column(varchar_type, slot_desc->is_nullable());
        const std::string& column_from_path = columns_from_path[i];
        Slice s(column_from_path.c_str(), column_from_path.size());
        col->append_value_multiple_times(&s, size);
        chunk->append_column(std::move(col), slot_desc->id());
    }
}

StatusOr<ChunkPtr> FileScanner::materialize(const starrocks::vectorized::ChunkPtr& src,
                                            starrocks::vectorized::ChunkPtr& cast) {
    SCOPED_RAW_TIMER(&_counter->materialize_ns);

    if (cast->num_rows() == 0) {
        return cast;
    }

    // materialize
    ChunkPtr dest_chunk = std::make_shared<Chunk>();

    int ctx_index = 0;
    int before_rows = cast->num_rows();
    vectorized::Column::Filter filter(cast->num_rows(), 1);

    // CREATE ROUTINE LOAD routine_load_job_1
    // on table COLUMNS (k1,k2,k3=k1)
    // The column k3 and k1 will pointer to the same entity.
    // The k3 should be copied to avoid this case.
    // column_pointers is a hashset to check the repeatability.
    HashSet<uintptr_t> column_pointers;
    for (const auto& slot : _dest_tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }

        int dest_index = ctx_index++;
        ExprContext* ctx = _dest_expr_ctx[dest_index];
        ASSIGN_OR_RETURN(auto col, ctx->evaluate(cast.get()));
        auto col_pointer = reinterpret_cast<uintptr_t>(col.get());
        if (column_pointers.contains(col_pointer)) {
            col = col->clone();
        } else {
            column_pointers.emplace(col_pointer);
        }

        col = ColumnHelper::unfold_const_column(slot->type(), cast->num_rows(), col);

        // The column builder in ctx->evaluate may build column as non-nullable.
        // See be/src/column/column_builder.h#L79.
        if (!col->is_nullable()) {
            col = ColumnHelper::cast_to_nullable_column(col);
        }

        dest_chunk->append_column(col, slot->id());

        if (src != nullptr && col->is_nullable() && col->has_null()) {
            if (_strict_mode && _dest_slot_desc_mappings[dest_index] != nullptr) {
                ColumnPtr& src_col = src->get_column_by_slot_id(_dest_slot_desc_mappings[dest_index]->id());

                for (int i = 0; i < col->size(); ++i) {
                    if (!col->is_null(i) || src_col->is_null(i) || !filter[i]) {
                        continue;
                    }

                    filter[i] = 0;
                    _error_counter++;

                    // avoid print too many debug log
                    if (_error_counter > 50) {
                        continue;
                    }
                    std::stringstream error_msg;
                    error_msg << "Value '" << src_col->debug_item(i) << "' is out of range. "
                              << "The type of '" << slot->col_name() << "' is " << slot->type().debug_string();
                    _state->append_error_msg_to_file(src->debug_row(i), error_msg.str());
                }
            }
        }
    }

    dest_chunk->filter(filter);
    _counter->num_rows_filtered += (before_rows - dest_chunk->num_rows());

    return dest_chunk;
}

Status FileScanner::create_sequential_file(const TBrokerRangeDesc& range_desc, const TNetworkAddress& address,
                                           const TBrokerScanRangeParams& params,
                                           std::shared_ptr<SequentialFile>* file) {
    CompressionTypePB compression = CompressionTypePB::DEFAULT_COMPRESSION;
    if (range_desc.format_type == TFileFormatType::FORMAT_JSON) {
        compression = CompressionTypePB::NO_COMPRESSION;
    } else if (range_desc.format_type == TFileFormatType::FORMAT_CSV_PLAIN) {
        compression = CompressionTypePB::NO_COMPRESSION;
    } else if (range_desc.format_type == TFileFormatType::FORMAT_CSV_GZ) {
        compression = CompressionTypePB::GZIP;
    } else if (range_desc.format_type == TFileFormatType::FORMAT_CSV_BZ2) {
        compression = CompressionTypePB::BZIP2;
    } else if (range_desc.format_type == TFileFormatType::FORMAT_CSV_LZ4_FRAME) {
        compression = CompressionTypePB::LZ4_FRAME;
    } else if (range_desc.format_type == TFileFormatType::FORMAT_CSV_DEFLATE) {
        compression = CompressionTypePB::DEFLATE;
    } else if (range_desc.format_type == TFileFormatType::FORMAT_CSV_ZSTD) {
        compression = CompressionTypePB::ZSTD;
    } else {
        return Status::NotSupported("Unsupported compression algorithm: " + std::to_string(range_desc.format_type));
    }

    std::shared_ptr<SequentialFile> src_file;
    switch (range_desc.file_type) {
    case TFileType::FILE_LOCAL: {
        ASSIGN_OR_RETURN(src_file, FileSystem::Default()->new_sequential_file(range_desc.path));
        break;
    }
    case TFileType::FILE_STREAM: {
        auto pipe = _state->exec_env()->load_stream_mgr()->get(range_desc.load_id);
        if (pipe == nullptr) {
            std::stringstream ss("Invalid or outdated load id ");
            range_desc.load_id.printTo(ss);
            return Status::InternalError(std::string(ss.str()));
        }
        bool non_blocking_read = false;
        if (params.__isset.non_blocking_read) {
            non_blocking_read = params.non_blocking_read;
        }
        auto stream = std::make_shared<StreamLoadPipeInputStream>(std::move(pipe), non_blocking_read);
        src_file = std::make_shared<SequentialFile>(std::move(stream), "stream-load-pipe");
        break;
    }
    case TFileType::FILE_BROKER: {
        if (params.__isset.use_broker && !params.use_broker) {
            ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(range_desc.path, FSOptions(&params)));
            ASSIGN_OR_RETURN(auto file, fs->new_sequential_file(range_desc.path));
            src_file = std::shared_ptr<SequentialFile>(std::move(file));
            break;
        } else {
            int64_t timeout_ms = _state->query_options().query_timeout * 1000 / 4;
            timeout_ms = std::max(timeout_ms, static_cast<int64_t>(DEFAULT_TIMEOUT_MS));
            BrokerFileSystem fs_broker(address, params.properties, timeout_ms);
            ASSIGN_OR_RETURN(auto broker_file, fs_broker.new_sequential_file(range_desc.path));
            src_file = std::shared_ptr<SequentialFile>(std::move(broker_file));
            break;
        }
    }
    }
    if (compression == CompressionTypePB::NO_COMPRESSION) {
        *file = src_file;
        return Status::OK();
    }

    using DecompressorPtr = std::shared_ptr<StreamCompression>;
    std::unique_ptr<StreamCompression> dec;
    RETURN_IF_ERROR(StreamCompression::create_decompressor(compression, &dec));
    auto stream = std::make_unique<io::CompressedInputStream>(src_file->stream(), DecompressorPtr(dec.release()));
    *file = std::make_shared<SequentialFile>(std::move(stream), range_desc.path);
    return Status::OK();
}

Status FileScanner::create_random_access_file(const TBrokerRangeDesc& range_desc, const TNetworkAddress& address,
                                              const TBrokerScanRangeParams& params, CompressionTypePB compression,
                                              std::shared_ptr<RandomAccessFile>* file) {
    std::shared_ptr<RandomAccessFile> src_file;
    switch (range_desc.file_type) {
    case TFileType::FILE_LOCAL: {
        ASSIGN_OR_RETURN(src_file, FileSystem::Default()->new_random_access_file(range_desc.path));
        break;
    }
    case TFileType::FILE_BROKER: {
        if (params.__isset.use_broker && !params.use_broker) {
            ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(range_desc.path, FSOptions(&params)));
            ASSIGN_OR_RETURN(auto file, fs->new_random_access_file(RandomAccessFileOptions(), range_desc.path));
            src_file = std::shared_ptr<RandomAccessFile>(std::move(file));
            break;
        } else {
            int64_t timeout_ms = _state->query_options().query_timeout * 1000 / 4;
            timeout_ms = std::max(timeout_ms, static_cast<int64_t>(DEFAULT_TIMEOUT_MS));
            BrokerFileSystem fs_broker(address, params.properties, timeout_ms);
            ASSIGN_OR_RETURN(auto broker_file, fs_broker.new_random_access_file(range_desc.path));
            src_file = std::shared_ptr<RandomAccessFile>(std::move(broker_file));
            break;
        }
    }
    case TFileType::FILE_STREAM:
        return Status::NotSupported("Does not support create random-access file from file stream");
    }
    if (compression == CompressionTypePB::NO_COMPRESSION) {
        *file = src_file;
        return Status::OK();
    } else {
        return Status::NotSupported("Does not support compressed random-access file");
    }
}

} // namespace starrocks::vectorized
