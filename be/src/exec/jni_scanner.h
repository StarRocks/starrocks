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

#pragma once

#include "column/chunk.h"
#include "common/logging.h"
#include "common/status.h"
#include "hdfs_scanner.h"
#include "jni.h"
#include "runtime/runtime_state.h"

namespace starrocks {

struct JniScannerProfile {
    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* io_counter = nullptr;
    RuntimeProfile::Counter* scan_ranges = nullptr;
    RuntimeProfile::Counter* open_timer = nullptr;
    RuntimeProfile::Counter* io_timer = nullptr;
    RuntimeProfile::Counter* fill_chunk_timer = nullptr;
};

class JniScanner : public HdfsScanner {
public:
    JniScanner(std::string factory_class, std::map<std::string, std::string> params)
            : _jni_scanner_params(std::move(params)), _jni_scanner_factory_class(std::move(factory_class)) {}

    ~JniScanner() override { finalize(); }

    Status do_open(RuntimeState* runtime_state) override;
    void do_update_counter(HdfsScanProfile* profile) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;

private:
    static Status _check_jni_exception(JNIEnv* _jni_env, const std::string& message);

    Status _init_jni_table_scanner(JNIEnv* _jni_env, RuntimeState* runtime_state);

    void _init_profile(const HdfsScannerParams& scanner_params);

    Status _init_jni_method(JNIEnv* _jni_env);

    Status _get_next_chunk(JNIEnv* _jni_env, long* chunk_meta);

    template <LogicalType type, typename CppType>
    Status _append_primitive_data(long num_rows, ColumnPtr& column);

    template <LogicalType type, typename CppType>
    Status _append_decimal_data(long num_rows, ColumnPtr& column, const std::string& slot_name,
                                const TypeDescriptor& slot_type);

    template <LogicalType type>
    Status _append_string_data(long num_rows, ColumnPtr& column);

    Status _fill_chunk(JNIEnv* _jni_env, ChunkPtr* chunk);

    // Status _fill_column(JNIEnv* _jni_env, int num_rows, ColumnPtr& column, const std::string& slot_name,
    //                     const TypeDescriptor& slot_type);

    template <LogicalType type, typename CppType>
    void _append_data(Column* column, CppType& value);

    Status _release_off_heap_table(JNIEnv* _jni_env);

    JniScannerProfile _profile;

    jclass _jni_scanner_cls;
    jobject _jni_scanner_obj;
    jmethodID _jni_scanner_open;
    jmethodID _jni_scanner_get_next_chunk;
    jmethodID _jni_scanner_close;
    jmethodID _jni_scanner_release_column;
    jmethodID _jni_scanner_release_table;

    std::map<std::string, std::string> _jni_scanner_params;
    std::string _jni_scanner_factory_class;

    long* _chunk_meta_ptr;
    int _chunk_meta_index;

    long next_chunk_meta() { return _chunk_meta_ptr[_chunk_meta_index++]; }
};
} // namespace starrocks
