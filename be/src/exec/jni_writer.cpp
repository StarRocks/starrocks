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

#include "jni_writer.h"

#include "exec/pipeline/sink/paimon_table_sink_operator.h"
#include "runtime/decimalv2_value.h"
#include "udf/java/java_udf.h"

namespace starrocks {

Status JniWriter::do_init(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(detect_java_runtime());
    _jni_env = JVMFunctionHelper::getInstance().getEnv();
    RETURN_IF_ERROR(_init_jni_table_writer(_jni_env, runtime_state));
    RETURN_IF_ERROR(_init_jni_method(_jni_env));
    return Status::OK();
}

Status JniWriter::write(RuntimeState* runtime_state, const ChunkPtr& chunk) {
    if (_jni_env == nullptr) {
        LOG(WARNING) << "Cannot get jni environment when preparing to write.";
        return Status::Unknown("NULL JNI ENV");
    }
    jobject jni_array_list = _jni_env->NewObject(_jni_array_list_class, _jni_array_list_init);
    RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(_jni_env, "Failed to new java array list."));
    Columns result_columns(chunk->num_columns());

    for (int col = 0; col < chunk->num_columns(); col++) {
        ASSIGN_OR_RETURN(result_columns[col], _output_expr[col]->evaluate(chunk.get()));

        jobjectArray objectArray = _jni_env->NewObjectArray(chunk->num_rows(), _jni_object_class, nullptr);

        auto column_type = _output_expr[col]->root()->type().type;
        if (column_type == TYPE_INT) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_int32();
                jobject java_element = _jni_env->NewObject(_jni_integer_class, _jni_integer_init, value);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_BIGINT) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_int64();
                jobject java_element = _jni_env->NewObject(_jni_long_class, _jni_long_init, value);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_SMALLINT) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_int16();
                jobject java_element = _jni_env->NewObject(_jni_short_class, _jni_short_init, value);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_TINYINT) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_int16();
                jobject java_element = _jni_env->NewObject(_jni_byte_class, _jni_byte_init, value);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_CHAR) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_slice().to_string();
                jstring java_element = _jni_env->NewStringUTF(value.c_str());
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_VARCHAR) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_slice().to_string();
                jstring java_element = _jni_env->NewStringUTF(value.c_str());
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_DOUBLE) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_double();
                jobject java_element = _jni_env->NewObject(_jni_double_class, _jni_double_init, value);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_FLOAT) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_float();
                jobject java_element = _jni_env->NewObject(_jni_float_class, _jni_float_init, value);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_BOOLEAN) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_uint8();
                bool bool_value = value == 0 ? 0 : 1;
                jobject java_element = _jni_env->NewObject(_jni_bool_class, _jni_bool_init, bool_value);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_DATE) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_int32() - date::UNIX_EPOCH_JULIAN;
                jobject java_element = _jni_env->NewObject(_jni_date_class, _jni_date_init, value);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_DATETIME) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_int64();
                // cast Julian date to timestamp, copied from time_type.h, and transfer the copied result to timestamp
                auto timestamp =
                        (((value >> 40) & 0x3FFFFF) - date::UNIX_EPOCH_JULIAN) * SECS_PER_DAY * USECS_PER_MILLIS +
                        (value & 0xFFFFFFFFFFLL) / USECS_PER_MILLIS;
                jobject java_element = _jni_env->NewObject(_jni_long_class, _jni_long_init, timestamp);
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_DECIMAL64) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = result_columns[col]->get(row).get_int64();
                auto precision = _output_expr[col]->root()->type().precision;
                auto scale = _output_expr[col]->root()->type().scale;
                std::string decimal_value =
                        std::to_string(value) + " " + std::to_string(precision) + " " + std::to_string(scale);
                jstring java_element = _jni_env->NewStringUTF(decimal_value.c_str());
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else if (column_type == TYPE_DECIMAL128) {
            for (auto row = 0; row < chunk->num_rows(); row++) {
                if (result_columns[col]->get(row).is_null()) {
                    continue;
                }
                auto value = int128_to_string(result_columns[col]->get(row).get_int128());
                auto precision = _output_expr[col]->root()->type().precision;
                auto scale = _output_expr[col]->root()->type().scale;
                std::string decimal_value = value + " " + std::to_string(precision) + " " + std::to_string(scale);
                jstring java_element = _jni_env->NewStringUTF(decimal_value.c_str());
                _jni_env->SetObjectArrayElement(objectArray, row, java_element);
                _jni_env->DeleteLocalRef(java_element);
            }
        } else {
            LOG(WARNING) << "Column type " << column_type << " does not supported.";
        }
        _jni_env->CallBooleanMethod(jni_array_list, _jni_array_list_add, objectArray);
        RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(_jni_env, "Failed to add java array list."));
        _jni_env->DeleteLocalRef(objectArray);
    }

    _jni_env->CallObjectMethod(_jni_writer_obj, _jni_writer_write, jni_array_list);
    RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(_jni_env, "Failed to write a chunk."));
    _jni_env->DeleteLocalRef(jni_array_list);
    return Status::OK();
}

Status JniWriter::commit(RuntimeState* runtime_state) {
    jstring json_mess_list = (jstring)_jni_env->CallObjectMethod(_jni_writer_obj, _jni_writer_commit);
    RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(_jni_env, "Failed to get commit message."));
    _json_mess_list = jstring_to_string(_jni_env, json_mess_list);
    _jni_env->DeleteLocalRef(json_mess_list);
    return Status::OK();
}

std::string JniWriter::get_commit_message() {
    return _json_mess_list;
}

Status JniWriter::_init_jni_table_writer(JNIEnv* jni_env, RuntimeState* runtime_state) {
    jclass writer_factory_class = jni_env->FindClass(_jni_writer_factory_class.c_str());
    jmethodID writer_factory_constructor = jni_env->GetMethodID(writer_factory_class, "<init>", "()V");
    jobject writer_factory_obj = jni_env->NewObject(writer_factory_class, writer_factory_constructor);
    // Paimon writer uses jni scanner framework, so the writer class should be called 'getScannerClass'.
    jmethodID get_writer_method = jni_env->GetMethodID(writer_factory_class, "getScannerClass", "()Ljava/lang/Class;");
    _jni_writer_cls = (jclass)jni_env->CallObjectMethod(writer_factory_obj, get_writer_method);
    RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(jni_env, "Failed to init the writer class."));
    jni_env->DeleteLocalRef(writer_factory_class);
    jni_env->DeleteLocalRef(writer_factory_obj);

    jmethodID writer_constructor = jni_env->GetMethodID(_jni_writer_cls, "<init>", "(Ljava/util/Map;)V");
    RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(jni_env, "Failed to get a writer class constructor."));

    jclass hashmap_class = jni_env->FindClass("java/util/HashMap");
    jmethodID hashmap_constructor = jni_env->GetMethodID(hashmap_class, "<init>", "(I)V");
    jobject hashmap_object = jni_env->NewObject(hashmap_class, hashmap_constructor, _jni_writer_params.size());
    jmethodID hashmap_put =
            jni_env->GetMethodID(hashmap_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(jni_env, "Failed to get the HashMap methods."));

    LOG(INFO) << "Initializing a jni writer.";
    for (const auto& it : _jni_writer_params) {
        jstring key = jni_env->NewStringUTF(it.first.c_str());
        jstring value = jni_env->NewStringUTF(it.second.c_str());
        jni_env->CallObjectMethod(hashmap_object, hashmap_put, key, value);
        jni_env->DeleteLocalRef(key);
        jni_env->DeleteLocalRef(value);
    }
    jni_env->DeleteLocalRef(hashmap_class);

    _jni_writer_obj = jni_env->NewObject(_jni_writer_cls, writer_constructor, hashmap_object);
    jni_env->DeleteLocalRef(hashmap_object);
    DCHECK(_jni_writer_obj != nullptr);
    RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(jni_env, "Failed to initialize a writer instance."));

    return Status::OK();
}

Status JniWriter::_init_jni_method(JNIEnv* jni_env) {
    // init jmethod
    _jni_writer_write = jni_env->GetMethodID(_jni_writer_cls, "write", "(Ljava/util/List;)V");
    _jni_writer_commit = jni_env->GetMethodID(_jni_writer_cls, "commit", "()Ljava/lang/String;");
    RETURN_IF_ERROR(JVMFunctionHelper::_check_jni_exception(jni_env, "Failed to get jni writer method"));

    _jni_array_list_class = jni_env->FindClass("java/util/ArrayList");
    _jni_array_list_init = jni_env->GetMethodID(_jni_array_list_class, "<init>", "()V");
    _jni_array_list_add = jni_env->GetMethodID(_jni_array_list_class, "add", "(Ljava/lang/Object;)Z");
    _jni_object_class = jni_env->FindClass("java/lang/Object");
    _jni_string_class = jni_env->FindClass("java/lang/String");
    _jni_short_class = jni_env->FindClass("java/lang/Short");
    _jni_integer_class = jni_env->FindClass("java/lang/Integer");
    _jni_long_class = jni_env->FindClass("java/lang/Long");
    _jni_bool_class = jni_env->FindClass("java/lang/Boolean");
    _jni_byte_class = jni_env->FindClass("java/lang/Byte");
    _jni_float_class = jni_env->FindClass("java/lang/Float");
    _jni_double_class = jni_env->FindClass("java/lang/Double");
    _jni_char_class = jni_env->FindClass("java/lang/Character");
    _jni_decimal_class = jni_env->FindClass("java/math/BigDecimal");
    _jni_big_integer_class = jni_env->FindClass("java/math/BigInteger");
    _jni_math_context_class = jni_env->FindClass("java/math/MathContext");
    _jni_date_class = jni_env->FindClass("java/lang/Integer");

    _jni_short_init = jni_env->GetMethodID(_jni_short_class, "<init>", "(S)V");
    _jni_integer_init = jni_env->GetMethodID(_jni_integer_class, "<init>", "(I)V");
    _jni_long_init = jni_env->GetMethodID(_jni_long_class, "<init>", "(J)V");
    _jni_bool_init = jni_env->GetMethodID(_jni_bool_class, "<init>", "(Z)V");
    _jni_byte_init = jni_env->GetMethodID(_jni_byte_class, "<init>", "(B)V");
    _jni_float_init = jni_env->GetMethodID(_jni_float_class, "<init>", "(F)V");
    _jni_double_init = jni_env->GetMethodID(_jni_double_class, "<init>", "(D)V");
    _jni_char_init = jni_env->GetMethodID(_jni_char_class, "<init>", "(C)V");
    _jni_decimal_init = jni_env->GetMethodID(_jni_decimal_class, "<init>", "(Ljava/math/BigInteger;I)V");
    _jni_big_integer_init = jni_env->GetMethodID(_jni_big_integer_class, "<init>", "([B)V");
    _jni_date_init = jni_env->GetMethodID(_jni_date_class, "<init>", "(I)V");
    _jni_math_context_init = _jni_env->GetMethodID(_jni_math_context_class, "<init>", "(I)V");
    _jni_decimal_round =
            _jni_env->GetMethodID(_jni_decimal_class, "round", "(Ljava/math/MathContext;)Ljava/math/BigDecimal;");

    return Status::OK();
}

void JniWriter::close(RuntimeState* runtime_state) noexcept {
    if (_jni_writer_obj != nullptr) {
        _jni_env->DeleteLocalRef(_jni_writer_obj);
        _jni_writer_obj = nullptr;
    }
    if (_jni_writer_cls != nullptr) {
        _jni_env->DeleteLocalRef(_jni_writer_cls);
        _jni_writer_cls = nullptr;
    }
    if (_jni_array_list_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_array_list_class);
        _jni_array_list_class = nullptr;
    }
    if (_jni_object_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_object_class);
        _jni_object_class = nullptr;
    }
    if (_jni_string_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_string_class);
        _jni_string_class = nullptr;
    }
    if (_jni_short_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_short_class);
        _jni_short_class = nullptr;
    }
    if (_jni_integer_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_integer_class);
        _jni_integer_class = nullptr;
    }
    if (_jni_long_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_long_class);
        _jni_long_class = nullptr;
    }
    if (_jni_bool_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_bool_class);
        _jni_bool_class = nullptr;
    }
    if (_jni_byte_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_byte_class);
        _jni_byte_class = nullptr;
    }
    if (_jni_float_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_float_class);
        _jni_float_class = nullptr;
    }
    if (_jni_double_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_double_class);
        _jni_double_class = nullptr;
    }
    if (_jni_char_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_char_class);
        _jni_char_class = nullptr;
    }
    if (_jni_date_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_date_class);
        _jni_date_class = nullptr;
    }
    if (_jni_decimal_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_decimal_class);
        _jni_decimal_class = nullptr;
    }
    if (_jni_big_integer_class != nullptr) {
        _jni_env->DeleteLocalRef(_jni_big_integer_class);
        _jni_big_integer_class = nullptr;
    }
}

std::string JniWriter::jstring_to_string(JNIEnv* jni_env, jstring jstr) {
    const char* cstr = jni_env->GetStringUTFChars(jstr, nullptr);
    size_t length = jni_env->GetStringUTFLength(jstr);
    std::string str(cstr, length);
    jni_env->ReleaseStringUTFChars(jstr, cstr);
    return str;
}

// seen https://stackoverflow.com/questions/45608424/atoi-for-int128-t-type
std::string JniWriter::int128_to_string(int128_t value) {
    std::ostringstream oss;
    if (value == 0) {
        return "0";
    }
    bool is_negative = false;
    if (value < 0) {
        is_negative = true;
        value = -value;
    }

    while (value > 0) {
        int digit = value % 10;
        oss << static_cast<char>('0' + digit);
        value /= 10;
    }

    std::string result = oss.str();
    std::reverse(result.begin(), result.end());

    if (is_negative) {
        result = "-" + result;
    }

    return result;
}
} // namespace starrocks
