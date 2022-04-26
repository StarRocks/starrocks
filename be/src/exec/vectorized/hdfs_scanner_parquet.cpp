// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "exec/vectorized/hdfs_scanner_parquet.h"

#include "exec/vectorized/hdfs_scan_node.h"
#include "formats/parquet/file_reader.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

class HdfsParquetProfile {
public:
    // read & decode
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

    void init(RuntimeProfile* root);
};

static const std::string kParquetProfileSectionPrefix = "Parquet";

void HdfsParquetProfile::init(RuntimeProfile* root) {
    ADD_TIMER(root, kParquetProfileSectionPrefix);
    level_decode_timer = ADD_CHILD_TIMER(root, "LevelDecodeTime", kParquetProfileSectionPrefix);
    value_decode_timer = ADD_CHILD_TIMER(root, "ValueDecodeTime", kParquetProfileSectionPrefix);

    page_read_timer = ADD_CHILD_TIMER(root, "PageReadTime", kParquetProfileSectionPrefix);
    footer_read_timer = ADD_CHILD_TIMER(root, "ReaderInitFooterRead", kParquetProfileSectionPrefix);
    column_reader_init_timer = ADD_CHILD_TIMER(root, "ReaderInitColumnReaderInit", kParquetProfileSectionPrefix);

    group_chunk_read_timer = ADD_CHILD_TIMER(root, "GroupChunkRead", kParquetProfileSectionPrefix);
    group_dict_filter_timer = ADD_CHILD_TIMER(root, "GroupDictFilter", kParquetProfileSectionPrefix);
    group_dict_decode_timer = ADD_CHILD_TIMER(root, "GroupDictDecode", kParquetProfileSectionPrefix);
}

Status HdfsParquetScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    HdfsScanProfile* profile = scanner_params.profile;
    // initialized once.
    if (profile != nullptr && profile->parquet_profile == nullptr) {
        profile->parquet_profile = profile->pool->add(new HdfsParquetProfile());
        profile->parquet_profile->init(profile->runtime_profile);
    }
    return Status::OK();
}

void HdfsParquetScanner::do_update_counter(HdfsScanProfile* profile) {
    HdfsParquetProfile* parquet_profile = profile->parquet_profile;
    if (parquet_profile != nullptr) {
        COUNTER_UPDATE(parquet_profile->value_decode_timer, _stats.value_decode_ns);
        COUNTER_UPDATE(parquet_profile->level_decode_timer, _stats.level_decode_ns);
        COUNTER_UPDATE(parquet_profile->page_read_timer, _stats.page_read_ns);
        COUNTER_UPDATE(parquet_profile->footer_read_timer, _stats.footer_read_ns);
        COUNTER_UPDATE(parquet_profile->column_reader_init_timer, _stats.column_reader_init_ns);
        COUNTER_UPDATE(parquet_profile->group_chunk_read_timer, _stats.group_chunk_read_ns);
        COUNTER_UPDATE(parquet_profile->group_dict_filter_timer, _stats.group_dict_filter_ns);
        COUNTER_UPDATE(parquet_profile->group_dict_decode_timer, _stats.group_dict_decode_ns);
    }
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

} // namespace starrocks::vectorized
