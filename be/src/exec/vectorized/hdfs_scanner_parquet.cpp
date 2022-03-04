// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

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
    _reader = std::make_shared<parquet::FileReader>(runtime_state->chunk_size(), _file.get(),
                                                    _scanner_params.scan_ranges[0]->file_length);
    SCOPED_RAW_TIMER(&_stats.reader_init_ns);
    RETURN_IF_ERROR(_reader->init(_file_read_param));
    return Status::OK();
}

Status HdfsParquetScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    Status status = _reader->get_next(chunk);
    return status;
}

void HdfsParquetScanner::do_close(RuntimeState* runtime_state) noexcept {
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
