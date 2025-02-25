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
#include "exec/paimon_writer.h"
#include "hdfs_scanner.h"
#include "jni.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class JniWriter : public PaimonWriter {
public:
    JniWriter(std::string factory_class, std::map<std::string, std::string> params,
              std::vector<ExprContext*> output_expr, std::vector<std::string> data_column_types)
            : _output_expr(std::move(output_expr)),
              _data_column_types(std::move(data_column_types)),
              _jni_writer_factory_class(std::move(factory_class)),
              _jni_writer_params(std::move(params)) {}
    ~JniWriter() override = default;

    Status do_init(RuntimeState* runtime_state) override;

    Status write(RuntimeState* runtime_state, const ChunkPtr& chunk) override;
    Status commit(RuntimeState* runtime_state) override;
    void close(RuntimeState* runtime_state) noexcept override;
    void set_output_expr(std::vector<ExprContext*> output_expr) override { _output_expr = output_expr; };

    std::string get_commit_message() override;

private:
    Status _init_jni_table_writer(JNIEnv* jni_env, RuntimeState* runtime_state);
    Status _init_jni_method(JNIEnv* jni_env);
    std::string jstring_to_string(JNIEnv* jni_env, jstring jstr);
    std::string int128_to_string(int128_t value);

    std::string _json_mess_list;

    JNIEnv* _jni_env;
    std::vector<ExprContext*> _output_expr;
    std::vector<std::string> _data_column_types;

    jclass _jni_writer_cls = nullptr;
    jclass _jni_array_list_class = nullptr;

    jobject _jni_writer_obj = nullptr;

    jmethodID _jni_writer_write = nullptr;
    jmethodID _jni_writer_commit = nullptr;
    jmethodID _jni_array_list_init = nullptr;
    jmethodID _jni_array_list_add = nullptr;
    jmethodID _jni_short_init = nullptr;
    jmethodID _jni_integer_init = nullptr;
    jmethodID _jni_long_init = nullptr;
    jmethodID _jni_bool_init = nullptr;
    jmethodID _jni_byte_init = nullptr;
    jmethodID _jni_float_init = nullptr;
    jmethodID _jni_double_init = nullptr;
    jmethodID _jni_char_init = nullptr;
    jmethodID _jni_decimal_init = nullptr;
    jmethodID _jni_big_integer_init = nullptr;
    jmethodID _jni_date_init = nullptr;
    jmethodID _jni_math_context_init = nullptr;
    jmethodID _jni_decimal_round = nullptr;

    jclass _jni_object_class = nullptr;
    jclass _jni_string_class = nullptr;
    jclass _jni_short_class = nullptr;
    jclass _jni_integer_class = nullptr;
    jclass _jni_long_class = nullptr;
    jclass _jni_bool_class = nullptr;
    jclass _jni_byte_class = nullptr;
    jclass _jni_float_class = nullptr;
    jclass _jni_double_class = nullptr;
    jclass _jni_char_class = nullptr;
    jclass _jni_decimal_class = nullptr;
    jclass _jni_date_class = nullptr;
    jclass _jni_big_integer_class = nullptr;
    jclass _jni_math_context_class = nullptr;

    std::string _jni_writer_factory_class;
    std::map<std::string, std::string> _jni_writer_params;
};

} // namespace starrocks
