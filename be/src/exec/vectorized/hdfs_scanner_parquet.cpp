// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/hdfs_scanner_parquet.h"

#include "exec/parquet/file_reader.h"
#include "exec/vectorized/hdfs_scan_node.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

Status HdfsParquetScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return Status::OK();
}

Status HdfsParquetScanner::do_open(RuntimeState* runtime_state) {
    // create file reader
    _reader = std::make_shared<parquet::FileReader>(runtime_state->chunk_size(), _scanner_params.fs.get(),
                                                    _scanner_params.scan_ranges[0]->file_length);
#ifndef BE_TEST
    SCOPED_TIMER(_scanner_params.parent->_reader_init_timer);
#endif
    RETURN_IF_ERROR(_reader->init(_file_read_param));
    return Status::OK();
}

Status HdfsParquetScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    Status status = _reader->get_next(chunk);
    return status;
}

void HdfsParquetScanner::update_counter() {
    HdfsScanner::update_counter();

#ifndef BE_TEST
    COUNTER_UPDATE(_scanner_params.parent->_rows_read_counter, _stats.raw_rows_read);
    COUNTER_UPDATE(_scanner_params.parent->_expr_filter_timer, _stats.expr_filter_ns);
    COUNTER_UPDATE(_scanner_params.parent->_io_timer, _stats.io_ns);
    COUNTER_UPDATE(_scanner_params.parent->_io_counter, _stats.io_count);
    COUNTER_UPDATE(_scanner_params.parent->_bytes_read_counter, _stats.bytes_read);
    COUNTER_UPDATE(_scanner_params.parent->_column_read_timer, _stats.column_read_ns);
    COUNTER_UPDATE(_scanner_params.parent->_column_convert_timer, _stats.column_convert_ns);

    const auto& parquet_profile = _scanner_params.parent->_parquet_profile;

    COUNTER_UPDATE(parquet_profile.value_decode_timer, _stats.value_decode_ns);
    COUNTER_UPDATE(parquet_profile.level_decode_timer, _stats.level_decode_ns);
    COUNTER_UPDATE(parquet_profile.page_read_timer, _stats.page_read_ns);
    COUNTER_UPDATE(parquet_profile.footer_read_timer, _stats.footer_read_ns);
    COUNTER_UPDATE(parquet_profile.column_reader_init_timer, _stats.column_reader_init_ns);
    COUNTER_UPDATE(parquet_profile.group_chunk_read_timer, _stats.group_chunk_read_ns);
    COUNTER_UPDATE(parquet_profile.group_dict_filter_timer, _stats.group_dict_filter_ns);
    COUNTER_UPDATE(parquet_profile.group_dict_decode_timer, _stats.group_dict_decode_ns);
#endif
}

void HdfsParquetScanner::do_close(RuntimeState* runtime_state) noexcept {
    update_counter();
    _reader.reset();
}

static const std::string kParquetProfileSectionPrefix = "Parquet";

void HdfsParquetProfile::init(RuntimeProfile* root) {
    if (_toplev != nullptr) return;
    _toplev = ADD_TIMER(root, kParquetProfileSectionPrefix);
    level_decode_timer = ADD_CHILD_TIMER(root, "LevelDecodeTime", kParquetProfileSectionPrefix);
    value_decode_timer = ADD_CHILD_TIMER(root, "ValueDecodeTime", kParquetProfileSectionPrefix);

    page_read_timer = ADD_CHILD_TIMER(root, "PageReadTime", kParquetProfileSectionPrefix);
    footer_read_timer = ADD_CHILD_TIMER(root, "ReaderInitFooterRead", kParquetProfileSectionPrefix);
    column_reader_init_timer = ADD_CHILD_TIMER(root, "ReaderInitColumnReaderInit", kParquetProfileSectionPrefix);

    group_chunk_read_timer = ADD_CHILD_TIMER(root, "GroupChunkRead", kParquetProfileSectionPrefix);
    group_dict_filter_timer = ADD_CHILD_TIMER(root, "GroupDictFilter", kParquetProfileSectionPrefix);
    group_dict_decode_timer = ADD_CHILD_TIMER(root, "GroupDictDecode", kParquetProfileSectionPrefix);
}

} // namespace starrocks::vectorized
