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

#include "exec/hdfs_scanner_parquet.h"

#include "exec/hdfs_scanner.h"
#include "exec/iceberg/iceberg_delete_builder.h"
#include "formats/parquet/file_reader.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace starrocks {

static const std::string kParquetProfileSectionPrefix = "Parquet";

Status HdfsParquetScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    if (!scanner_params.deletes.empty()) {
        std::unique_ptr<IcebergDeleteBuilder> iceberg_delete_builder(
                new IcebergDeleteBuilder(scanner_params.fs, scanner_params.path, scanner_params.conjunct_ctxs,
                                         scanner_params.materialize_slots, &_need_skip_rowids));
        for (const auto& tdelete_file : scanner_params.deletes) {
            RETURN_IF_ERROR(iceberg_delete_builder->build_parquet(runtime_state->timezone(), *tdelete_file));
        }
    }
    return Status::OK();
}

void HdfsParquetScanner::do_update_counter(HdfsScanProfile* profile) {
    RuntimeProfile::Counter* request_bytes_read = nullptr;
    RuntimeProfile::Counter* request_bytes_read_uncompressed = nullptr;
    RuntimeProfile::Counter* level_decode_timer = nullptr;
    RuntimeProfile::Counter* value_decode_timer = nullptr;
    RuntimeProfile::Counter* page_read_timer = nullptr;

    // reader init
    RuntimeProfile::Counter* footer_read_timer = nullptr;
    RuntimeProfile::Counter* column_reader_init_timer = nullptr;

    // dict filter
    RuntimeProfile::Counter* group_chunk_read_timer = nullptr;
    RuntimeProfile::Counter* group_dict_filter_timer = nullptr;
    RuntimeProfile::Counter* group_dict_decode_timer = nullptr;
    RuntimeProfile::Counter* build_iceberg_pos_filter_timer = nullptr;

    // page statistics
    RuntimeProfile::Counter* has_page_statistics = nullptr;
    // page skip
    RuntimeProfile::Counter* page_skip = nullptr;

    RuntimeProfile* root = profile->runtime_profile;
    ADD_COUNTER(root, kParquetProfileSectionPrefix, TUnit::UNIT);
    request_bytes_read = ADD_CHILD_COUNTER(root, "RequestBytesRead", TUnit::BYTES, kParquetProfileSectionPrefix);
    request_bytes_read_uncompressed =
            ADD_CHILD_COUNTER(root, "RequestBytesReadUncompressed", TUnit::BYTES, kParquetProfileSectionPrefix);

    level_decode_timer = ADD_CHILD_TIMER(root, "LevelDecodeTime", kParquetProfileSectionPrefix);
    value_decode_timer = ADD_CHILD_TIMER(root, "ValueDecodeTime", kParquetProfileSectionPrefix);

    page_read_timer = ADD_CHILD_TIMER(root, "PageReadTime", kParquetProfileSectionPrefix);
    footer_read_timer = ADD_CHILD_TIMER(root, "ReaderInitFooterRead", kParquetProfileSectionPrefix);
    column_reader_init_timer = ADD_CHILD_TIMER(root, "ReaderInitColumnReaderInit", kParquetProfileSectionPrefix);

    group_chunk_read_timer = ADD_CHILD_TIMER(root, "GroupChunkRead", kParquetProfileSectionPrefix);
    group_dict_filter_timer = ADD_CHILD_TIMER(root, "GroupDictFilter", kParquetProfileSectionPrefix);
    group_dict_decode_timer = ADD_CHILD_TIMER(root, "GroupDictDecode", kParquetProfileSectionPrefix);
    build_iceberg_pos_filter_timer = ADD_CHILD_TIMER(root, "BuildIcebergPosFilter", kParquetProfileSectionPrefix);

    has_page_statistics = ADD_CHILD_COUNTER(root, "HasPageStatistics", TUnit::UNIT, kParquetProfileSectionPrefix);
    page_skip = ADD_CHILD_COUNTER(root, "PageSkipCounter", TUnit::UNIT, kParquetProfileSectionPrefix);

    COUNTER_UPDATE(request_bytes_read, _stats.request_bytes_read);
    COUNTER_UPDATE(request_bytes_read_uncompressed, _stats.request_bytes_read_uncompressed);
    COUNTER_UPDATE(value_decode_timer, _stats.value_decode_ns);
    COUNTER_UPDATE(level_decode_timer, _stats.level_decode_ns);
    COUNTER_UPDATE(page_read_timer, _stats.page_read_ns);
    COUNTER_UPDATE(footer_read_timer, _stats.footer_read_ns);
    COUNTER_UPDATE(column_reader_init_timer, _stats.column_reader_init_ns);
    COUNTER_UPDATE(group_chunk_read_timer, _stats.group_chunk_read_ns);
    COUNTER_UPDATE(group_dict_filter_timer, _stats.group_dict_filter_ns);
    COUNTER_UPDATE(group_dict_decode_timer, _stats.group_dict_decode_ns);
<<<<<<< HEAD
    COUNTER_UPDATE(build_iceberg_pos_filter_timer, _stats.build_iceberg_pos_filter_ns);
=======

    int64_t page_stats = _stats.has_page_statistics ? 1 : 0;
    COUNTER_UPDATE(has_page_statistics, page_stats);
    COUNTER_UPDATE(page_skip, _stats.page_skip);
>>>>>>> cd754b6ef ([Enhancement] more profile message (backport #28562 #28524) (#28578))
}

Status HdfsParquetScanner::do_open(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(open_random_access_file());
    // create file reader
    _reader = std::make_shared<parquet::FileReader>(runtime_state->chunk_size(), _file.get(), _file->get_size().value(),
                                                    _shared_buffered_input_stream.get(), &_need_skip_rowids);

    SCOPED_RAW_TIMER(&_stats.reader_init_ns);
    RETURN_IF_ERROR(_reader->init(&_scanner_ctx));
    return Status::OK();
}

Status HdfsParquetScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    Status status = _reader->get_next(chunk);
    return status;
}

void HdfsParquetScanner::do_close(RuntimeState* runtime_state) noexcept {
    _reader.reset();
}

} // namespace starrocks
