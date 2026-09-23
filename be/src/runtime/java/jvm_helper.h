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

#include <jni.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "base/logging.h"
#include "base/string/slice.h"
#include "base/utility/defer_op.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fmt/core.h"
#include "runtime/java/jvm_class.h"

#define DEFINE_JAVA_PRIM_TYPE(TYPE) \
    jclass _class_##TYPE;           \
    jmethodID _value_of_##TYPE;     \
    jmethodID _val_##TYPE;

#define DECLARE_NEW_BOX(PRIM_CLAZZ, TYPE, CLAZZ) \
    jobject new##CLAZZ(TYPE value);              \
    TYPE val##TYPE(jobject obj);                 \
    jclass TYPE##_class() { return _class_##PRIM_CLAZZ; }

namespace starrocks {

struct ListMeta {
    JVMClass* list_class;
    JVMClass* array_list_class;

    jmethodID list_get;
    jmethodID list_size;
    jmethodID list_add;
};

// Restrictions on use:
// can only be used in pthread, not in bthread
// thread local helper
class JVMHelper {
public:
    static JVMHelper& getInstance();
    static std::pair<JNIEnv*, JVMHelper&> getInstanceWithEnv();
    JVMHelper(const JVMHelper&) = delete;

    JNIEnv* getEnv() { return _env; }

    std::string array_to_string(jobject object);
    bool equals(jobject obj1, jobject obj2);
    std::string to_string(jobject obj);
    std::string to_cxx_string(jstring str);
    std::string dumpExceptionString(jthrowable throwable);
    jmethodID getToStringMethod(jclass clazz);
    StatusOr<jstring> to_jstring(const std::string& str);
    jmethodID getMethod(jclass clazz, const std::string& method, const std::string& sig);
    jmethodID getStaticMethod(jclass clazz, const std::string& method, const std::string& sig);

    jobject create_array(int sz);
    jobject create_object_array(jobject o, int num_rows);
    jobjectArray build_object_array(jclass clazz, jobject* arr, int sz);

    DECLARE_NEW_BOX(boolean, uint8_t, Boolean)
    DECLARE_NEW_BOX(byte, int8_t, Byte)
    DECLARE_NEW_BOX(short, int16_t, Short)
    DECLARE_NEW_BOX(int, int32_t, Integer)
    DECLARE_NEW_BOX(long, int64_t, Long)
    DECLARE_NEW_BOX(float, float, Float)
    DECLARE_NEW_BOX(double, double, Double)

    const ListMeta& list_meta() const { return _list_meta; }
    JVMClass& map_class() { return *_map_class; }

    jobject newString(const char* data, size_t size);
    jobject newBigDecimal(const std::string& s);
    jobject newBigDecimal(int64_t unscaled, int scale);
    Slice sliceVal(jstring jstr, std::string* buffer);

    jclass object_class() { return _object_class; }
    jclass object_array_class() { return _object_array_class; }
    jclass string_clazz() { return _string_class; }
    jclass big_decimal_class() { return _big_decimal_class; }
    jclass local_date_class() { return _local_date_class; }
    jclass local_datetime_class() { return _local_datetime_class; }
    jclass direct_buffer_class() { return _direct_buffer_class; }
    jmethodID direct_buffer_clear() { return _direct_buffer_clear; }

    jobject newLocalDateFromEpochDay(int64_t epoch_day);
    int64_t valLocalDateToEpochDay(jobject obj);

    static std::string to_jni_class_name(const std::string& name);

private:
    JVMHelper() { _init(); }
    void _init();
    Status _check_exception_status();

private:
    inline static thread_local JNIEnv* _env = nullptr;

    DEFINE_JAVA_PRIM_TYPE(boolean);
    DEFINE_JAVA_PRIM_TYPE(byte);
    DEFINE_JAVA_PRIM_TYPE(short);
    DEFINE_JAVA_PRIM_TYPE(int);
    DEFINE_JAVA_PRIM_TYPE(long);
    DEFINE_JAVA_PRIM_TYPE(float);
    DEFINE_JAVA_PRIM_TYPE(double);

    jclass _object_class;
    jclass _object_array_class;
    jclass _string_class;
    jclass _jarrays_class;
    jclass _exception_util_class;
    jclass _big_decimal_class;
    jclass _local_date_class;
    jclass _local_datetime_class;

    jmethodID _object_to_string;
    jmethodID _object_equals;
    jmethodID _arrays_to_string;
    jmethodID _string_construct_with_bytes;
    jmethodID _big_decimal_ctor_string;
    jmethodID _big_decimal_value_of_ll;
    jmethodID _local_date_of_epoch_day;
    jmethodID _local_date_to_epoch_day;

    ListMeta _list_meta;
    JVMClass* _map_class = nullptr;

    jobject _utf8_charsets;

    jclass _direct_buffer_class;
    jmethodID _direct_buffer_clear;
};

// local object reference guard.
// The objects inside are automatically call DeleteLocalRef in the life object.
#define LOCAL_REF_GUARD(lref)                                                     \
    ::starrocks::DeferOp VARNAME_LINENUM(guard)([&lref]() {                       \
        if (lref) {                                                               \
            ::starrocks::JVMHelper::getInstance().getEnv()->DeleteLocalRef(lref); \
            lref = nullptr;                                                       \
        }                                                                         \
    })

#define LOCAL_REF_GUARD_ENV(env, lref)                           \
    ::starrocks::DeferOp VARNAME_LINENUM(guard)([&lref, env]() { \
        if (lref) {                                              \
            env->DeleteLocalRef(lref);                           \
            lref = nullptr;                                      \
        }                                                        \
    })

#define RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, prefix)                          \
    if (auto e = env->ExceptionOccurred()) {                                            \
        LOCAL_REF_GUARD(e);                                                             \
        std::string err = ::starrocks::JVMHelper::getInstance().dumpExceptionString(e); \
        env->ExceptionClear();                                                          \
        return ::starrocks::Status::InternalError(fmt::format("{}, {}", prefix, err));  \
    }

#define RETURN_ERROR_IF_JNI_EXCEPTION(env) RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "JNI Exception")

#define RETURN_IF_JNI_EXCEPTION(env, errmsg, ret)                                                              \
    if (jthrowable jthr = env->ExceptionOccurred()) {                                                          \
        LOCAL_REF_GUARD(jthr);                                                                                 \
        std::string msg =                                                                                      \
                fmt::format("{},{}", errmsg, ::starrocks::JVMHelper::getInstance().dumpExceptionString(jthr)); \
        LOG(WARNING) << msg;                                                                                   \
        env->ExceptionClear();                                                                                 \
        return ret;                                                                                            \
    }

// Used for Java direct buffers over native memory.
// DirectByteBuffer does not hold ownership of this memory space.
class DirectByteBuffer {
public:
    static constexpr const char* JNI_CLASS_NAME = "java/nio/ByteBuffer";

    DirectByteBuffer(void* data, int capacity);
    ~DirectByteBuffer();

    DirectByteBuffer(const DirectByteBuffer&) = delete;
    DirectByteBuffer& operator=(const DirectByteBuffer& other) = delete;

    DirectByteBuffer(DirectByteBuffer&& other) noexcept {
        _handle = other._handle;
        _data = other._data;
        _capacity = other._capacity;

        other._handle = nullptr;
        other._data = nullptr;
        other._capacity = 0;
    }

    DirectByteBuffer& operator=(DirectByteBuffer&& other) noexcept {
        DirectByteBuffer tmp(std::move(other));
        std::swap(this->_handle, tmp._handle);
        std::swap(this->_data, tmp._data);
        std::swap(this->_capacity, tmp._capacity);
        return *this;
    }

    jobject handle() { return _handle; }
    void* data() { return _data; }
    int capacity() { return _capacity; }

private:
    jobject _handle;
    void* _data;
    int _capacity;
};

class JavaListStub {
public:
    JavaListStub(jobject list) : _list(list) {}
    Status add(jobject element);
    StatusOr<jobject> get(int index);
    StatusOr<size_t> size();

private:
    jobject _list;
};

} // namespace starrocks
