// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "column/chunk.h"
#include "common/logging.h"
#include "common/status.h"
#include "hdfs_scanner.h"
#include "jni.h"
#include "runtime/runtime_state.h"
#include "udf/java/java_udf.h"

namespace starrocks::vectorized {

struct HuiMORScannerContext {
    std::string base_path;
    std::string hive_column_names;
    std::string hive_column_types;
    std::vector<std::string> required_fields;
    std::string instant_time;
    std::vector<std::string> delta_log_paths;
    std::string data_file_path;
    long data_file_lenth;
    std::string serde_lib;
    std::string input_format;
};

struct HuiMORScannerProfile {
    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* io_timer = nullptr;
    RuntimeProfile::Counter* io_counter = nullptr;
    RuntimeProfile::Counter* fill_chunk_timer = nullptr;
    RuntimeProfile::Counter* open_timer = nullptr;
};

class HudiMORScanner final : public HdfsScanner {
public:
    HudiMORScanner(HuiMORScannerContext context) : _scan_ctx(std::move(context)) {}

    ~HudiMORScanner() override { finalize(); }

    Status open(RuntimeState* runtime_state) override;
    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;

    // if execution threads has been changed, we have to reset jni env
    // because jni env can not be used across threads.
    Status reset_jni_env();

private:
    void _init_profile(const HdfsScannerParams& scanner_params);

    Status _init_hudi_mor_scanner(RuntimeState* state);

    Status _has_next(bool* result);

    Status _get_next_chunk(long* chunk_meta);

    Status _process_null_value(Column* column, SlotDescriptor* slot_desc);

    Status _fill_chunk(long chunk_meta, ChunkPtr* chunk);

    template <PrimitiveType type, typename CppType>
    void _append_data(Column* column, CppType& value);

    HuiMORScannerContext _scan_ctx;

    HuiMORScannerProfile _profile;

    JNIEnv* _jni_env = nullptr;
    jobject _hudi_mor_scanner;
    jclass _hudi_mor_scanner_cls;
    jmethodID _scanner_has_next;
    jmethodID _scanner_get_next_chunk;
    jmethodID _scanner_column_release;
};
} // namespace starrocks::vectorized
