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

#include "udf/java/java_udf.h"

#include <iterator>
#include <memory>
#include <sstream>
#include <string>

#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "common/status.h"
#include "exprs/function_context.h"
#include "fmt/core.h"
#include "jni.h"
#include "runtime/java/java_env.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "udf/java/java_native_method.h"
#include "udf/java/java_udf_reflection.h"
#include "udf/java/type_traits.h"

// find a jclass and return a global jclass ref
#define JNI_FIND_CLASS(clazz_name)                                                                 \
    [](const char* name) {                                                                         \
        auto clazz = _env->FindClass(name);                                                        \
        CHECK(clazz != nullptr) << "not found class" << name << " plz check JDK and jni-packages"; \
        auto g_clazz = _env->NewGlobalRef(clazz);                                                  \
        _env->DeleteLocalRef(clazz);                                                               \
        return (jclass)g_clazz;                                                                    \
    }(clazz_name)

#define ADD_NUMBERIC_CLASS(prim_clazz, clazz, sign)                                                           \
    {                                                                                                         \
        _class_##prim_clazz = JNI_FIND_CLASS("java/lang/" #clazz);                                            \
        CHECK(_class_##prim_clazz);                                                                           \
        _value_of_##prim_clazz =                                                                              \
                _env->GetStaticMethodID(_class_##prim_clazz, "valueOf", "(" #sign ")Ljava/lang/" #clazz ";"); \
        CHECK(_value_of_##prim_clazz) << "Not Found"                                                          \
                                      << "(" #sign ")Ljava/lang/" #clazz ";";                                 \
        _val_##prim_clazz = _env->GetMethodID(_class_##prim_clazz, #prim_clazz "Value", "()" #sign);          \
        CHECK(_val_##prim_clazz);                                                                             \
    }

#define DEFINE_NEW_BOX(prim_clazz, cxx_type, CLAZZ, CallType)                                    \
    jobject JVMFunctionHelper::new##CLAZZ(cxx_type value) {                                      \
        return _env->CallStaticObjectMethod(_class_##prim_clazz, _value_of_##prim_clazz, value); \
    }                                                                                            \
    cxx_type JVMFunctionHelper::val##cxx_type(jobject obj) {                                     \
        return _env->Call##CallType##Method(obj, _val_##prim_clazz);                             \
    }

namespace starrocks {

constexpr const char* CLASS_UDF_HELPER_NAME = "com.starrocks.udf.UDFHelper";
constexpr const char* CLASS_NATIVE_METHOD_HELPER_NAME = "com.starrocks.utils.NativeMethodHelper";

constexpr const char* BATCH_UPDATE_SIGNATURE =
        "(Ljava/lang/Object;Ljava/lang/reflect/Method;Lcom/starrocks/udf/FunctionStates;[I[Ljava/lang/Object;)V";
constexpr const char* BATCH_CALL_SIGNATURE =
        "(Ljava/lang/Object;Ljava/lang/reflect/Method;I[Ljava/lang/Object;)[Ljava/lang/Object;";
constexpr const char* BATCH_CALL_NO_ARGS_SIGNATURE =
        "(Ljava/lang/Object;Ljava/lang/reflect/Method;I)[Ljava/lang/Object;";
constexpr const char* BATCH_UPDATE_STATE_SIGNATURE =
        "(Ljava/lang/Object;Ljava/lang/reflect/Method;[Ljava/lang/Object;)V";
constexpr const char* BATCH_UPDATE_IF_NOT_NULL_SIGNATURE =
        "(Ljava/lang/Object;Ljava/lang/reflect/Method;Lcom/starrocks/udf/FunctionStates;[I[Ljava/lang/Object;)V";
constexpr const char* INT_BATCH_CALL_SIGNATURE = "([Ljava/lang/Object;Ljava/lang/reflect/Method;I)[I";

constexpr const char* CREATE_BOXED_PRIMI_SIGNATURE = "(ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)[Ljava/lang/Object;";
constexpr const char* CREATE_BOXED_STRING_SIGNATURE =
        "(ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)[Ljava/lang/Object;";
constexpr const char* CREATE_BOXED_LIST_SIGNATURE =
        "(ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;[Ljava/lang/Object;)[Ljava/lang/Object;";
constexpr const char* CREATE_BOXED_MAP_SIGNATURE =
        "(ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;[Ljava/lang/Object;[Ljava/lang/Object;)[Ljava/lang/Object;";
// (numRows, parentNullBuf, recordClass, fieldArrays) -> Object[]
constexpr const char* CREATE_BOXED_STRUCT_SIGNATURE =
        "(ILjava/nio/ByteBuffer;Ljava/lang/Class;[Ljava/lang/Object;)[Ljava/lang/Object;";
// (numRows, boxedResult, columnAddr, typeDesc) -> void.
// Unified dispatcher: writes the result jobject into the native column tree. Handles
// STRUCT / ARRAY / MAP / DECIMAL / scalar by recursing on the Java side using the
// UdfTypeDesc tree. The BE constructs the type desc once per UDF and just calls this
// helper once per query batch.
constexpr const char* WRITE_RESULT_SIGNATURE = "(ILjava/lang/Object;JLcom/starrocks/udf/UdfTypeDesc;)V";

#define INIT_STATIC_METHOD(target, clazz, name, signature)    \
    target = _env->GetStaticMethodID(clazz, name, signature); \
    CHECK(target != nullptr) << "not found method:" << name << " plz check your jni-packages";

#define INIT_HELPER_METHOD(target, name, signature) INIT_STATIC_METHOD(target, _udf_helper_class, name, signature);

#define SET_METHOD_ID(target, clazz, name, signature)   \
    target = _env->GetMethodID(clazz, name, signature); \
    DCHECK(target != nullptr);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
static JNINativeMethod java_native_methods[] = {
        {"resizeStringData", "(JI)J", (void*)&JavaNativeMethods::resizeStringData},
        {"resize", "(JI)V", (void*)&JavaNativeMethods::resize},
        {"getColumnLogicalType", "(J)I", (void*)&JavaNativeMethods::getColumnLogicalType},
        {"getAddrs", "(J)[J", (void*)&JavaNativeMethods::getAddrs},
        {"getStructFieldAddrs", "(J)[J", (void*)&JavaNativeMethods::getStructFieldAddrs},
        {"memoryTrackerMalloc", "(J)J", (void*)&JavaNativeMethods::memory_malloc},
        {"memoryTrackerFree", "(J)V", (void*)&JavaNativeMethods::memory_free},
};
#pragma GCC diagnostic pop

JavaUDAFUniqueContext* get_java_udaf_context(FunctionContext* ctx) {
    if (ctx == nullptr) {
        return nullptr;
    }
    return reinterpret_cast<JavaUDAFUniqueContext*>(ctx->get_function_state(FunctionContext::THREAD_LOCAL));
}

void attach_java_udaf_context(FunctionContext* ctx, std::unique_ptr<JavaUDAFUniqueContext> udaf_ctx) {
    DCHECK(ctx != nullptr);
    DCHECK(udaf_ctx != nullptr);
    auto* old_ctx = get_java_udaf_context(ctx);
    DCHECK(old_ctx == nullptr) << "duplicate Java UDAF context attach";
    if (old_ctx != nullptr) {
        delete old_ctx;
    }
    ctx->set_function_state(FunctionContext::THREAD_LOCAL, udaf_ctx.release());
}

void clear_java_udaf_states(FunctionContext* ctx) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    if (udaf_ctx == nullptr || udaf_ctx->states == nullptr) {
        return;
    }

    auto env = JVMFunctionHelper::getInstance().getEnv();
    udaf_ctx->states->clear(ctx, env);
}

void destroy_java_udaf_context(FunctionContext* ctx) {
    if (ctx == nullptr) {
        return;
    }

    auto* udaf_ctx = get_java_udaf_context(ctx);
    if (udaf_ctx == nullptr) {
        return;
    }

    clear_java_udaf_states(ctx);
    ctx->set_function_state(FunctionContext::THREAD_LOCAL, nullptr);
    delete udaf_ctx;
}

StatusOr<jobject> MapMeta::newLocalInstance(jobject keys, jobject values) const {
    JNIEnv* env = getJNIEnv();
    auto res = env->NewObject(immutable_map_class->clazz(), immutable_map_constructor, keys, values);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

JVMFunctionHelper& JVMFunctionHelper::getInstance() {
    if (_env == nullptr) {
        _env = getJNIEnv();
        CHECK(_env != nullptr) << "couldn't got a JNIEnv";
    }
    static JVMFunctionHelper helper;
    return helper;
}

std::pair<JNIEnv*, JVMFunctionHelper&> JVMFunctionHelper::getInstanceWithEnv() {
    auto& instance = getInstance();
    return {instance.getEnv(), instance};
}

void JVMFunctionHelper::_init() {
    _object_class = JNI_FIND_CLASS("java/lang/Object");
    _object_array_class = JNI_FIND_CLASS("[Ljava/lang/Object;");
    _string_class = JNI_FIND_CLASS("java/lang/String");
    _jarrays_class = JNI_FIND_CLASS("java/util/Arrays");
    _exception_util_class = JNI_FIND_CLASS("org/apache/commons/lang3/exception/ExceptionUtils");
    _big_decimal_class = JNI_FIND_CLASS("java/math/BigDecimal");

    CHECK(_object_class);
    CHECK(_string_class);
    CHECK(_jarrays_class);
    CHECK(_exception_util_class);
    CHECK(_big_decimal_class);
    _big_decimal_ctor_string = _env->GetMethodID(_big_decimal_class, "<init>", "(Ljava/lang/String;)V");
    CHECK(_big_decimal_ctor_string);
    _big_decimal_value_of_ll = _env->GetStaticMethodID(_big_decimal_class, "valueOf", "(JI)Ljava/math/BigDecimal;");
    CHECK(_big_decimal_value_of_ll);

    _local_date_class = JNI_FIND_CLASS("java/time/LocalDate");
    CHECK(_local_date_class);
    _local_date_of_epoch_day = _env->GetStaticMethodID(_local_date_class, "ofEpochDay", "(J)Ljava/time/LocalDate;");
    CHECK(_local_date_of_epoch_day);
    _local_date_to_epoch_day = _env->GetMethodID(_local_date_class, "toEpochDay", "()J");
    CHECK(_local_date_to_epoch_day);

    _local_datetime_class = JNI_FIND_CLASS("java/time/LocalDateTime");
    CHECK(_local_datetime_class);

    ADD_NUMBERIC_CLASS(boolean, Boolean, Z);
    ADD_NUMBERIC_CLASS(byte, Byte, B);
    ADD_NUMBERIC_CLASS(short, Short, S);
    ADD_NUMBERIC_CLASS(int, Integer, I);
    ADD_NUMBERIC_CLASS(long, Long, J);
    ADD_NUMBERIC_CLASS(float, Float, F);
    ADD_NUMBERIC_CLASS(double, Double, D);

    jclass charsets = JNI_FIND_CLASS("java/nio/charset/StandardCharsets");
    DCHECK(charsets != nullptr);
    auto fieldId = _env->GetStaticFieldID(charsets, "UTF_8", "Ljava/nio/charset/Charset;");
    DCHECK(fieldId != nullptr);

    auto charset = _env->GetStaticObjectField(charsets, fieldId);
    _utf8_charsets = _env->NewGlobalRef(charset);
    _env->DeleteLocalRef(charset);

    DCHECK(_utf8_charsets != nullptr);
    _string_construct_with_bytes = _env->GetMethodID(_string_class, "<init>", "([BLjava/nio/charset/Charset;)V");
    DCHECK(_string_construct_with_bytes != nullptr);

    // init JNI native methods
    std::string native_method_name = JVMFunctionHelper::to_jni_class_name(CLASS_NATIVE_METHOD_HELPER_NAME);
    jclass native_method_class = JNI_FIND_CLASS(native_method_name.c_str());
    DCHECK(native_method_class != nullptr);
    int res = _env->RegisterNatives(native_method_class, java_native_methods,
                                    sizeof(java_native_methods) / sizeof(java_native_methods[0]));

    // init UDF Helper
    std::string name = JVMFunctionHelper::to_jni_class_name(CLASS_UDF_HELPER_NAME);
    _udf_helper_class = JNI_FIND_CLASS(name.c_str());
    DCHECK(_udf_helper_class != nullptr);
    DCHECK_EQ(res, 0);

    INIT_HELPER_METHOD(_create_boxed_array, "createBoxedArray", "(IIZ[Ljava/nio/ByteBuffer;)[Ljava/lang/Object;");
    INIT_HELPER_METHOD(_create_boxed_decimal_array, "createBoxedDecimalArray",
                       "(IIILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)[Ljava/lang/Object;");
    INIT_HELPER_METHOD(_create_boxed_struct_array, "createBoxedStructArray", CREATE_BOXED_STRUCT_SIGNATURE);
    INIT_HELPER_METHOD(_write_result, "writeResult", WRITE_RESULT_SIGNATURE);

    // Cache the UdfTypeDesc class + constructor + field IDs for the BE-side
    // input boxing recursion (which reads children / recordClass per STRUCT slot)
    // and for the per-UDF type desc construction in _build_udf_func_desc.
    {
        std::string td_name = JVMFunctionHelper::to_jni_class_name("com.starrocks.udf.UdfTypeDesc");
        _udf_type_desc_class = JNI_FIND_CLASS(td_name.c_str());
        DCHECK(_udf_type_desc_class != nullptr);
        _udf_type_desc_ctor = _env->GetMethodID(_udf_type_desc_class, "<init>",
                                                "(I[Lcom/starrocks/udf/UdfTypeDesc;IILjava/lang/Class;)V");
        DCHECK(_udf_type_desc_ctor != nullptr);
        _udf_type_desc_record_class = _env->GetFieldID(_udf_type_desc_class, "recordClass", "Ljava/lang/Class;");
        _udf_type_desc_children =
                _env->GetFieldID(_udf_type_desc_class, "children", "[Lcom/starrocks/udf/UdfTypeDesc;");
        DCHECK(_udf_type_desc_record_class != nullptr && _udf_type_desc_children != nullptr);
    }
    INIT_HELPER_METHOD(_get_decimal_boxed_result, "getDecimalResultFromBoxedArray", "(IIIILjava/lang/Object;JZ)V");
    INIT_HELPER_METHOD(_bd_unscaled_long, "unscaledLong", "(Ljava/math/BigDecimal;II)J");
    INIT_HELPER_METHOD(_bd_unscaled_le_bytes, "unscaledLEBytes", "(Ljava/math/BigDecimal;III)[B");
    INIT_HELPER_METHOD(_batch_update, "batchUpdate", BATCH_UPDATE_SIGNATURE);
    INIT_HELPER_METHOD(_batch_call, "batchCall", BATCH_CALL_SIGNATURE);
    INIT_HELPER_METHOD(_batch_call_no_args, "batchCall", BATCH_CALL_NO_ARGS_SIGNATURE);
    INIT_HELPER_METHOD(_batch_update_state, "batchUpdateState", BATCH_UPDATE_STATE_SIGNATURE);
    INIT_HELPER_METHOD(_batch_update_if_not_null, "batchUpdateIfNotNull", BATCH_UPDATE_IF_NOT_NULL_SIGNATURE);
    INIT_HELPER_METHOD(_batch_create_bytebuf, "batchCreateDirectBuffer", "(J[II)[Ljava/lang/Object;");
    INIT_HELPER_METHOD(_int_batch_call, "batchCall", INT_BATCH_CALL_SIGNATURE);
    INIT_HELPER_METHOD(_get_boxed_result, "getResultFromBoxedArray", "(IILjava/lang/Object;J)V");
    INIT_HELPER_METHOD(_extract_keys_from_map, "extractKeyList", "(Ljava/util/Map;)Ljava/util/List;");
    INIT_HELPER_METHOD(_extract_values_from_map, "extractValueList", "(Ljava/util/Map;)Ljava/util/List;");
    // init UDF Helper convert method
    jmethodID tmp = nullptr;
    INIT_HELPER_METHOD(tmp, "createBoxedBoolArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<uint8_t>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedByteArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<int8_t>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedShortArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<int16_t>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedIntegerArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<int32_t>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedLongArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<int64_t>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedFloatArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<float>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedDoubleArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<double>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedStringArray", CREATE_BOXED_STRING_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<Slice>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedListArray", CREATE_BOXED_LIST_SIGNATURE);
    _method_map.emplace(TYPE_ARRAY_METHOD_ID, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedMapArray", CREATE_BOXED_MAP_SIGNATURE);
    _method_map.emplace(TYPE_MAP_METHOD_ID, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedLocalDateArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<DateValue>::id, tmp);
    INIT_HELPER_METHOD(tmp, "createBoxedLocalDateTimeArray", CREATE_BOXED_PRIMI_SIGNATURE);
    _method_map.emplace(JNIPrimTypeId<TimestampValue>::id, tmp);

    // LocalDateTime <-> packed int64 conversion stays in UDFHelper because the
    // packing is StarRocks-specific.
    INIT_HELPER_METHOD(_local_datetime_from_packed, "localDateTimeFromPackedTimestamp", "(J)Ljava/time/LocalDateTime;");
    INIT_HELPER_METHOD(_local_datetime_to_packed, "packedTimestampFromLocalDateTime", "(Ljava/time/LocalDateTime;)J");

    // init bytebuffer
    _direct_buffer_class = JNI_FIND_CLASS("java/nio/ByteBuffer");
    SET_METHOD_ID(_direct_buffer_clear, _direct_buffer_class, "clear", "()Ljava/nio/Buffer;");

    // init list meta
    auto list_clazz = JNI_FIND_CLASS("java/util/List");
    DCHECK(list_clazz);
    _list_meta.list_class = new JVMClass(list_clazz);
    auto array_list_clazz = JNI_FIND_CLASS("java/util/ArrayList");
    DCHECK(array_list_clazz);
    _list_meta.array_list_class = new JVMClass(array_list_clazz);

    SET_METHOD_ID(_list_meta.list_get, list_clazz, "get", "(I)Ljava/lang/Object;");
    SET_METHOD_ID(_list_meta.list_size, list_clazz, "size", "()I");
    SET_METHOD_ID(_list_meta.list_add, list_clazz, "add", "(Ljava/lang/Object;)Z");

    // init immutable map meta
    auto immutable_map_clazz = JNI_FIND_CLASS("com/starrocks/udf/ImmutableMap");
    DCHECK(immutable_map_clazz != nullptr);
    _map_meta.immutable_map_class = new JVMClass(immutable_map_clazz);
    auto map_clazz = JNI_FIND_CLASS("java/util/Map");
    DCHECK(map_clazz);
    _map_meta.map_class = new JVMClass(map_clazz);
    _map_meta.immutable_map_constructor =
            _env->GetMethodID(_map_meta.immutable_map_class->clazz(), "<init>", "(Ljava/util/List;Ljava/util/List;)V");

    name = JVMFunctionHelper::to_jni_class_name(UDAFStateList::clazz_name);
    jclass loaded_clazz = JNI_FIND_CLASS(name.c_str());
    _function_states_clazz = new JVMClass(loaded_clazz);
}

jobjectArray JVMFunctionHelper::_build_object_array(jclass clazz, jobject* arr, int sz) {
    jobjectArray res_arr = _env->NewObjectArray(sz, _object_array_class, nullptr);
    RETURN_IF_JNI_EXCEPTION(_env, "_build_object_array: NewObjectArray failed", nullptr);
    for (int i = 0; i < sz; ++i) {
        _env->SetObjectArrayElement(res_arr, i, arr[i]);
    }
    return res_arr;
}

JVMClass& JVMFunctionHelper::function_state_clazz() {
    return *_function_states_clazz;
}

#define CHECK_FUNCTION_EXCEPTION(_env, name)                   \
    if (auto e = _env->ExceptionOccurred()) {                  \
        LOCAL_REF_GUARD(e);                                    \
        _env->ExceptionClear();                                \
        LOG(WARNING) << "Exception happened when call " #name; \
        return "";                                             \
    }

#define RETURN_ERROR_IF_EXCEPTION(env, errmsg)                                   \
    if (jthrowable jthr = env->ExceptionOccurred()) {                            \
        LOCAL_REF_GUARD(jthr);                                                   \
        std::string msg = fmt::format(errmsg, helper.dumpExceptionString(jthr)); \
        LOG(WARNING) << msg;                                                     \
        env->ExceptionClear();                                                   \
        return Status::RuntimeError(msg);                                        \
    }

Status JVMFunctionHelper::_check_exception_status() {
    auto& helper = *this;
    RETURN_ERROR_IF_EXCEPTION(_env, "exception happened when invoke method: {}");
    return Status::OK();
}

std::string JVMFunctionHelper::array_to_string(jobject object) {
    _env->ExceptionClear();
    std::string value;
    jmethodID arrayToStringMethod =
            _env->GetStaticMethodID(_jarrays_class, "toString", "([Ljava/lang/Object;)Ljava/lang/String;");
    DCHECK(arrayToStringMethod != nullptr);
    jobject jstr = _env->CallStaticObjectMethod(_jarrays_class, arrayToStringMethod, object);
    LOCAL_REF_GUARD(jstr);
    CHECK_FUNCTION_EXCEPTION(_env, "array_to_string")
    value = to_cxx_string((jstring)jstr);
    return value;
}

std::string JVMFunctionHelper::to_string(jobject obj) {
    _env->ExceptionClear();
    std::string value;
    auto method = getToStringMethod(_object_class);
    auto res = _env->CallObjectMethod(obj, method);
    LOCAL_REF_GUARD(res);
    CHECK_FUNCTION_EXCEPTION(_env, "to_string")
    value = to_cxx_string((jstring)res);
    return value;
}

std::string JVMFunctionHelper::to_cxx_string(jstring str) {
    if (str == nullptr) {
        return "<null>";
    }
    std::string res;
    const char* charflow = _env->GetStringUTFChars((jstring)str, nullptr);
    res = charflow;
    _env->ReleaseStringUTFChars((jstring)str, charflow);
    return res;
}

std::string JVMFunctionHelper::dumpExceptionString(jthrowable throwable) {
    // toString
    auto get_stack_trace = _env->GetStaticMethodID(_exception_util_class, "getStackTrace",
                                                   "(Ljava/lang/Throwable;)Ljava/lang/String;");
    CHECK(get_stack_trace != nullptr) << "Not Found JNI method getStackTrace";
    jobject stack_traces = _env->CallStaticObjectMethod(_exception_util_class, get_stack_trace, (jobject)throwable);
    LOCAL_REF_GUARD(stack_traces);
    // don't call return if xxx, to avoid recursive
    _env->ExceptionClear();
    return to_cxx_string((jstring)stack_traces);
}

jmethodID JVMFunctionHelper::getToStringMethod(jclass clazz) {
    return _env->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
}

StatusOr<jstring> JVMFunctionHelper::to_jstring(const std::string& str) {
    auto res = _env->NewStringUTF(str.c_str());
    if (UNLIKELY(res == nullptr)) {
        _env->ExceptionClear();
        return Status::InternalError(fmt::format("NewStringUTF failed for: {}", str));
    }
    return res;
}

jmethodID JVMFunctionHelper::getMethod(jclass clazz, const std::string& method, const std::string& sig) {
    return _env->GetMethodID(clazz, method.c_str(), sig.c_str());
}

jmethodID JVMFunctionHelper::getStaticMethod(jclass clazz, const std::string& method, const std::string& sig) {
    return _env->GetStaticMethodID(clazz, method.c_str(), sig.c_str());
}

jobject JVMFunctionHelper::create_array(int sz) {
    auto res = _env->NewObjectArray(sz, _object_class, nullptr);
    RETURN_IF_JNI_EXCEPTION(_env, "create_array: NewObjectArray failed", nullptr);
    return res;
}

jobject JVMFunctionHelper::create_boxed_array(int type, int num_rows, bool nullable, DirectByteBuffer* buffs, int sz) {
    jobjectArray input_arr = _env->NewObjectArray(sz, _direct_buffer_class, nullptr);
    RETURN_IF_JNI_EXCEPTION(_env, "create_boxed_array: NewObjectArray failed", nullptr);
    LOCAL_REF_GUARD(input_arr);
    for (int i = 0; i < sz; ++i) {
        _env->SetObjectArrayElement(input_arr, i, buffs[i].handle());
    }
    jobject res =
            _env->CallStaticObjectMethod(_udf_helper_class, _create_boxed_array, type, num_rows, nullable, input_arr);
    RETURN_IF_JNI_EXCEPTION(_env, "create_boxed_array", nullptr);
    return res;
}

StatusOr<jobject> JVMFunctionHelper::create_boxed_decimal_array(int type, int scale, int num_rows, jobject null_buff,
                                                                jobject data_buff) {
    jobject res = _env->CallStaticObjectMethod(_udf_helper_class, _create_boxed_decimal_array, type, num_rows, scale,
                                               null_buff, data_buff);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(_env, "create_boxed_decimal_array");
    return res;
}

StatusOr<jobject> JVMFunctionHelper::create_boxed_struct_array(jclass record_class, int num_rows, jobject null_buff,
                                                               jobject field_arrays) {
    jobject res = _env->CallStaticObjectMethod(_udf_helper_class, _create_boxed_struct_array, num_rows, null_buff,
                                               record_class, field_arrays);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(_env, "create_boxed_struct_array");
    return res;
}

StatusOr<jobject> JVMFunctionHelper::new_udf_type_desc(jint logical_type, jobjectArray children, jint precision,
                                                       jint scale, jobject record_class) {
    jobject obj = _env->NewObject(_udf_type_desc_class, _udf_type_desc_ctor, logical_type, children, precision, scale,
                                  record_class);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(_env, "new_udf_type_desc");
    return obj;
}

Status JVMFunctionHelper::write_result(jobject result, int num_rows, jlong column_addr, jobject type_desc,
                                       bool /*error_if_overflow*/) {
    // Resize the outer column to num_rows so the Java helper's getAddrs returns
    // a buffer that's actually backed by num_rows worth of bytes. Without this
    // the Java side's Platform.copyMemory writes past the end of an empty null
    // buffer (the BE allocates a 0-sized column via create_column) and crashes.
    // For NullableColumn(StructColumn) this also cascades to each subfield, so
    // STRUCT field writes find their addresses lined up. ARRAY/MAP element
    // columns are still resized inside the Java drain helpers using the
    // computed total flattened length.
    auto* col = reinterpret_cast<Column*>(column_addr);
    col->resize(num_rows);

    // The error_if_overflow flag is currently ignored by the unified Java helper;
    // inner DECIMAL collection elements always report errors on overflow (matching
    // the pre-refactor writeElementsByType behavior). For top-level DECIMAL returns,
    // BE callers can route through get_result_from_boxed_array with explicit
    // precision/scale instead.
    _env->CallStaticVoidMethod(_udf_helper_class, _write_result, num_rows, result, column_addr, type_desc);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(_env, "write_result");
    return Status::OK();
}

jobject JVMFunctionHelper::create_object_array(jobject o, int num_rows) {
    jobjectArray res_arr = _env->NewObjectArray(num_rows, _object_array_class, o);
    RETURN_IF_JNI_EXCEPTION(_env, "create_object_array: NewObjectArray failed", nullptr);
    return res_arr;
}

jobject JVMFunctionHelper::batch_create_bytebuf(unsigned char* ptr, const uint32_t* offset, int begin, int end) {
    int size = end - begin;
    auto offsets = _env->NewIntArray(size + 1);
    RETURN_IF_JNI_EXCEPTION(_env, "batch_create_bytebuf: NewIntArray failed", nullptr);
    _env->SetIntArrayRegion(offsets, 0, size + 1, (const int32_t*)offset);
    LOCAL_REF_GUARD(offsets);
    auto res = _env->CallStaticObjectMethod(_udf_helper_class, _batch_create_bytebuf, ptr, offsets, size);
    RETURN_IF_JNI_EXCEPTION(_env, "batch_create_bytebuf", nullptr);
    return res;
}

void JVMFunctionHelper::batch_update_single(AggBatchCallStub* stub, int state, jobject* input, int cols, int rows) {
    auto obj = convert_handle_to_jobject(stub->ctx(), state);
    LOCAL_REF_GUARD(obj);
    stub->batch_update_single(rows, obj, input, cols);
}

void JVMFunctionHelper::batch_update(FunctionContext* ctx, jobject udaf, jobject update, jobject states, jobject* input,
                                     int cols) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    DCHECK(udaf_ctx != nullptr);
    DCHECK(udaf_ctx->states != nullptr);
    jobjectArray input_arr = _build_object_array(_object_array_class, input, cols);
    LOCAL_REF_GUARD(input_arr);
    _env->CallStaticVoidMethod(_udf_helper_class, _batch_update, udaf, update, udaf_ctx->states->handle(), states,
                               input_arr);
    CHECK_UDF_CALL_EXCEPTION(_env, ctx);
}

void JVMFunctionHelper::batch_update_state(FunctionContext* ctx, jobject udaf, jobject update, jobject* input,
                                           int cols) {
    jobjectArray input_arr = _build_object_array(_object_array_class, input, cols);
    LOCAL_REF_GUARD(input_arr);
    _env->CallStaticVoidMethod(_udf_helper_class, _batch_update_state, udaf, update, input_arr);
    CHECK_UDF_CALL_EXCEPTION(_env, ctx);
}

void JVMFunctionHelper::batch_update_if_not_null(FunctionContext* ctx, jobject udaf, jobject update, jobject states,
                                                 jobject* input, int cols) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    DCHECK(udaf_ctx != nullptr);
    DCHECK(udaf_ctx->states != nullptr);
    jobjectArray input_arr = _build_object_array(_object_array_class, input, cols);
    LOCAL_REF_GUARD(input_arr);
    _env->CallStaticVoidMethod(_udf_helper_class, _batch_update_if_not_null, udaf, update, udaf_ctx->states->handle(),
                               states, input_arr);
    CHECK_UDF_CALL_EXCEPTION(_env, ctx);
}

StatusOr<jobject> JVMFunctionHelper::batch_call(BatchEvaluateStub* stub, jobject* input, int cols, int rows) {
    return stub->batch_evaluate(rows, input, cols);
}

jobject JVMFunctionHelper::batch_call(FunctionContext* ctx, jobject caller, jobject method, jobject* input, int cols,
                                      int rows) {
    jobjectArray input_arr = _build_object_array(_object_array_class, input, cols);
    LOCAL_REF_GUARD(input_arr);
    auto res = _env->CallStaticObjectMethod(_udf_helper_class, _batch_call, caller, method, rows, input_arr);
    CHECK_UDF_CALL_EXCEPTION(_env, ctx);
    return res;
}

jobject JVMFunctionHelper::batch_call(FunctionContext* ctx, jobject caller, jobject method, int rows) {
    auto res = _env->CallStaticObjectMethod(_udf_helper_class, _batch_call_no_args, caller, method, rows);
    CHECK_UDF_CALL_EXCEPTION(_env, ctx);
    return res;
}

jobject JVMFunctionHelper::int_batch_call(FunctionContext* ctx, jobject callers, jobject method, int rows) {
    auto res = _env->CallStaticObjectMethod(_udf_helper_class, _int_batch_call, callers, method, rows);
    CHECK_UDF_CALL_EXCEPTION(_env, ctx);
    return res;
}

// Single-source dispatcher: routes DECIMAL types through the precision/scale-aware
// Java helper, everything else through the regular per-type writer. Splitting at the
// JNI boundary still pays off — the regular path skips the BigDecimal rescale loop —
// but BE callers see one entry point with sensible defaults for non-DECIMAL slots.
static void invoke_boxed_result(JNIEnv* env, jclass helper_class, jmethodID get_boxed_result,
                                jmethodID get_decimal_boxed_result, int type, int precision, int scale, Column* col,
                                jobject jcolumn, int rows, bool error_if_overflow) {
    col->resize(rows);
    if (is_decimalv3_field_type(static_cast<LogicalType>(type))) {
        env->CallStaticVoidMethod(helper_class, get_decimal_boxed_result, type, precision, scale, rows, jcolumn,
                                  reinterpret_cast<int64_t>(col), static_cast<jboolean>(error_if_overflow));
    } else {
        env->CallStaticVoidMethod(helper_class, get_boxed_result, type, rows, jcolumn, reinterpret_cast<int64_t>(col));
    }
}

Status JVMFunctionHelper::get_result_from_boxed_array(int type, Column* col, jobject jcolumn, int rows, int precision,
                                                      int scale, bool error_if_overflow) {
    invoke_boxed_result(_env, _udf_helper_class, _get_boxed_result, _get_decimal_boxed_result, type, precision, scale,
                        col, jcolumn, rows, error_if_overflow);
    RETURN_ERROR_IF_JNI_EXCEPTION(_env);
    return Status::OK();
}

void JVMFunctionHelper::get_result_from_boxed_array(FunctionContext* ctx, int type, Column* col, jobject jcolumn,
                                                    int rows, int precision, int scale, bool error_if_overflow) {
    invoke_boxed_result(_env, _udf_helper_class, _get_boxed_result, _get_decimal_boxed_result, type, precision, scale,
                        col, jcolumn, rows, error_if_overflow);
    CHECK_UDF_CALL_EXCEPTION(_env, ctx);
}

// convert UDAF ctx to jobject
jobject JVMFunctionHelper::convert_handle_to_jobject(FunctionContext* ctx, int state) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    DCHECK(udaf_ctx != nullptr);
    DCHECK(udaf_ctx->states != nullptr);
    auto* states = udaf_ctx->states.get();
    return states->get_state(ctx, _env, state);
}

jobject JVMFunctionHelper::convert_handles_to_jobjects(FunctionContext* ctx, jobject state_ids) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    DCHECK(udaf_ctx != nullptr);
    DCHECK(udaf_ctx->states != nullptr);
    auto* states = udaf_ctx->states.get();
    return states->get_state(ctx, _env, state_ids);
}

StatusOr<jobject> JVMFunctionHelper::extract_key_list(jobject map) {
    auto res = _env->CallStaticObjectMethod(_udf_helper_class, _extract_keys_from_map, map);
    RETURN_ERROR_IF_JNI_EXCEPTION(_env);
    return res;
}

StatusOr<jobject> JVMFunctionHelper::extract_val_list(jobject map) {
    auto res = _env->CallStaticObjectMethod(_udf_helper_class, _extract_values_from_map, map);
    RETURN_ERROR_IF_JNI_EXCEPTION(_env);
    return res;
}

DEFINE_NEW_BOX(boolean, uint8_t, Boolean, Boolean);
DEFINE_NEW_BOX(byte, int8_t, Byte, Byte);
DEFINE_NEW_BOX(short, int16_t, Short, Short);
DEFINE_NEW_BOX(int, int32_t, Integer, Int);
DEFINE_NEW_BOX(long, int64_t, Long, Long);
DEFINE_NEW_BOX(float, float, Float, Float);
DEFINE_NEW_BOX(double, double, Double, Double);

jobject JVMFunctionHelper::newString(const char* data, size_t size) {
    auto bytesArr = _env->NewByteArray(size);
    RETURN_IF_JNI_EXCEPTION(_env, "newString: NewByteArray failed", nullptr);
    LOCAL_REF_GUARD(bytesArr);
    _env->SetByteArrayRegion(bytesArr, 0, size, reinterpret_cast<const jbyte*>(data));
    jobject nstr = _env->NewObject(_string_class, _string_construct_with_bytes, bytesArr, _utf8_charsets);
    RETURN_IF_JNI_EXCEPTION(_env, "newString: NewObject failed", nullptr);
    return nstr;
}

jobject JVMFunctionHelper::newBigDecimal(const std::string& s) {
    jobject jstr = newString(s.data(), s.size());
    if (jstr == nullptr) {
        return nullptr;
    }
    LOCAL_REF_GUARD(jstr);
    jobject bd = _env->NewObject(_big_decimal_class, _big_decimal_ctor_string, jstr);
    RETURN_IF_JNI_EXCEPTION(_env, "newBigDecimal: NewObject failed", nullptr);
    return bd;
}

jobject JVMFunctionHelper::newBigDecimal(int64_t unscaled, int scale) {
    jobject bd = _env->CallStaticObjectMethod(_big_decimal_class, _big_decimal_value_of_ll,
                                              static_cast<jlong>(unscaled), static_cast<jint>(scale));
    RETURN_IF_JNI_EXCEPTION(_env, "newBigDecimal(long, int): CallStatic failed", nullptr);
    return bd;
}

jlong JVMFunctionHelper::unscaled_long(jobject big_decimal, int precision, int scale) {
    return _env->CallStaticLongMethod(_udf_helper_class, _bd_unscaled_long, big_decimal, static_cast<jint>(precision),
                                      static_cast<jint>(scale));
}

jbyteArray JVMFunctionHelper::unscaled_le_bytes(jobject big_decimal, int precision, int scale, int byte_width) {
    return (jbyteArray)_env->CallStaticObjectMethod(_udf_helper_class, _bd_unscaled_le_bytes, big_decimal,
                                                    static_cast<jint>(precision), static_cast<jint>(scale),
                                                    static_cast<jint>(byte_width));
}

// DateValue stores days as Julian day; LocalDate exposes them as days-since-1970-01-01.
// 2440588 is the Julian day number of the Unix epoch.
static constexpr jlong UNIX_EPOCH_JULIAN_DAYS = 2440588;

jobject JVMFunctionHelper::newLocalDate(int32_t julian) {
    jobject ld = _env->CallStaticObjectMethod(_local_date_class, _local_date_of_epoch_day,
                                              static_cast<jlong>(julian) - UNIX_EPOCH_JULIAN_DAYS);
    RETURN_IF_JNI_EXCEPTION(_env, "newLocalDate: ofEpochDay failed", nullptr);
    return ld;
}

int32_t JVMFunctionHelper::valLocalDate(jobject obj) {
    jlong epoch_day = _env->CallLongMethod(obj, _local_date_to_epoch_day);
    return static_cast<int32_t>(epoch_day + UNIX_EPOCH_JULIAN_DAYS);
}

jobject JVMFunctionHelper::newLocalDateTime(int64_t packed_timestamp) {
    jobject ldt = _env->CallStaticObjectMethod(_udf_helper_class, _local_datetime_from_packed,
                                               static_cast<jlong>(packed_timestamp));
    RETURN_IF_JNI_EXCEPTION(_env, "newLocalDateTime: localDateTimeFromPackedTimestamp failed", nullptr);
    return ldt;
}

int64_t JVMFunctionHelper::valLocalDateTime(jobject obj) {
    return _env->CallStaticLongMethod(_udf_helper_class, _local_datetime_to_packed, obj);
}

Slice JVMFunctionHelper::sliceVal(jstring jstr, std::string* buffer) {
    const size_t utf_length = _env->GetStringUTFLength(jstr);
    buffer->resize(utf_length);
    const size_t string_length = _env->GetStringLength(jstr);
    _env->GetStringUTFRegion(jstr, 0, string_length, buffer->data());
    return {buffer->data(), buffer->length()};
}

std::string JVMFunctionHelper::to_jni_class_name(const std::string& name) {
    std::string jni_class_name;
    auto inserter = std::inserter(jni_class_name, jni_class_name.end());
    std::transform(name.begin(), name.end(), inserter, [](auto ele) {
        if (ele == '.') {
            return '/';
        }
        return ele;
    });
    return jni_class_name;
}

void JVMFunctionHelper::clear(DirectByteBuffer* buffer, FunctionContext* ctx) {
    auto res = _env->CallNonvirtualObjectMethod(buffer->handle(), _direct_buffer_class, _direct_buffer_clear);
    LOCAL_REF_GUARD(res);
    CHECK_UDF_CALL_EXCEPTION(_env, ctx);
}

DirectByteBuffer::DirectByteBuffer(void* ptr, int capacity) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();
    auto lref = env->NewDirectByteBuffer(ptr, capacity);
    LOCAL_REF_GUARD(lref);
    _handle = env->NewGlobalRef(lref);
    _capacity = capacity;
    _data = ptr;
}

DirectByteBuffer::~DirectByteBuffer() {
    if (_handle != nullptr) {
        auto st = JavaEnv::GetInstance()->call_function_in_pthread([this]() {
            auto& helper = JVMFunctionHelper::getInstance();
            JNIEnv* env = helper.getEnv();
            env->DeleteGlobalRef(_handle);
            _handle = nullptr;
            return Status::OK();
        });
        LOG_IF(WARNING, !st.ok()) << "failed to delete direct byte buffer global ref: " << st;
    }
}

UDAFStateList::UDAFStateList(JavaGlobalRef&& handle, JavaGlobalRef&& get, JavaGlobalRef&& batch_get,
                             JavaGlobalRef&& add, JavaGlobalRef&& remove, JavaGlobalRef&& clear)
        : _handle(std::move(handle)),
          _get_method(std::move(get)),
          _batch_get_method(std::move(batch_get)),
          _add_method(std::move(add)),
          _remove_method(std::move(remove)),
          _clear_method(std::move(clear)) {
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    _get_method_id = env->FromReflectedMethod(_get_method.handle());
    _batch_get_method_id = env->FromReflectedMethod(_batch_get_method.handle());
    _add_method_id = env->FromReflectedMethod(_add_method.handle());
    _remove_method_id = env->FromReflectedMethod(_remove_method.handle());
    _clear_method_id = env->FromReflectedMethod(_clear_method.handle());
}

jobject UDAFStateList::get_state(FunctionContext* ctx, JNIEnv* env, int state_handle) {
    auto obj = env->CallObjectMethod(_handle.handle(), _get_method_id, state_handle);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
    return obj;
}

jobject UDAFStateList::get_state(FunctionContext* ctx, JNIEnv* env, jobject state_ids) {
    auto obj = env->CallObjectMethod(_handle.handle(), _batch_get_method_id, state_ids);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
    return obj;
}

int UDAFStateList::add_state(FunctionContext* ctx, JNIEnv* env, jobject state) {
    auto res = env->CallIntMethod(_handle.handle(), _add_method_id, state);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
    return res;
}

void UDAFStateList::remove(FunctionContext* ctx, JNIEnv* env, int state_handle) {
    env->CallVoidMethod(_handle.handle(), _remove_method_id, state_handle);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

void UDAFStateList::clear(FunctionContext* ctx, JNIEnv* env) {
    env->CallVoidMethod(_handle.handle(), _clear_method_id);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

JavaUDFContext::~JavaUDFContext() = default;
JavaUDAFSharedContext::~JavaUDAFSharedContext() = default;

int UDAFFunction::create() {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    jmethodID create = _ctx->ctx->create->get_method_id();
    auto obj = env->CallObjectMethod(_udaf_handle, create);
    LOCAL_REF_GUARD(obj);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    return _ctx->states->add_state(_function_context, env, obj);
}

void UDAFFunction::destroy(int state) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID destory = _ctx->ctx->destory->get_method_id();
    // call destroy
    env->CallVoidMethod(_udaf_handle, destory, obj);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    _ctx->states->remove(_function_context, env, state);
}

jvalue UDAFFunction::finalize(int state) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID finalize = _ctx->ctx->finalize->get_method_id();
    jvalue res;
    res.l = env->CallObjectMethod(_udaf_handle, finalize, obj);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    return res;
}

void AggBatchCallStub::batch_update_single(int num_rows, jobject state, jobject* input, int cols) {
    jvalue jni_inputs[3 + cols];
    jni_inputs[0].i = num_rows;
    jni_inputs[1].l = _caller;
    jni_inputs[2].l = state;
    for (int i = 0; i < cols; ++i) {
        jni_inputs[3 + i].l = input[i];
    }
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    env->CallStaticVoidMethodA(_stub_clazz.clazz(), env->FromReflectedMethod(_stub_method.handle()), jni_inputs);
    CHECK_UDF_CALL_EXCEPTION(env, _ctx);
}

StatusOr<jobject> BatchEvaluateStub::batch_evaluate(int num_rows, jobject* input, int cols) {
    jvalue jni_inputs[2 + cols];
    jni_inputs[0].i = num_rows;
    jni_inputs[1].l = _caller;
    for (int i = 0; i < cols; ++i) {
        jni_inputs[2 + i].l = input[i];
    }
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    auto res = env->CallStaticObjectMethodA(_stub_clazz.clazz(), env->FromReflectedMethod(_stub_method.handle()),
                                            jni_inputs);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

Status JavaListStub::add(jobject element) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    const auto& meta = helper.list_meta();
    env->CallVoidMethod(_list, meta.list_add, element);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return Status::OK();
}

StatusOr<jobject> JavaListStub::get(int index) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    const auto& meta = helper.list_meta();
    auto res = env->CallObjectMethod(_list, meta.list_get, index);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

StatusOr<size_t> JavaListStub::size() {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    const auto& meta = helper.list_meta();
    auto res = env->CallIntMethod(_list, meta.list_size);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

void UDAFFunction::update(jvalue* val) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    jmethodID update = _ctx->ctx->update->get_method_id();
    env->CallVoidMethodA(_udaf_handle, update, val);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
}

void UDAFFunction::merge(int state, jobject buffer) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID merge = _ctx->ctx->merge->get_method_id();
    env->CallVoidMethod(_udaf_handle, merge, obj, buffer);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
}

void UDAFFunction::serialize(int state, jobject buffer) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID serialize = _ctx->ctx->serialize->get_method_id();
    env->CallVoidMethod(_udaf_handle, serialize, obj, buffer);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
}

int UDAFFunction::serialize_size(int state) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID serialize_size = _ctx->ctx->serialize_size->get_method_id();
    int sz = env->CallIntMethod(obj, serialize_size);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    return sz;
}

void UDAFFunction::reset(int state) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);
    jmethodID reset = _ctx->ctx->reset->get_method_id();
    env->CallVoidMethod(_udaf_handle, reset, obj);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
}

jobject UDAFFunction::window_update_batch(int state, int peer_group_start, int peer_group_end, int frame_start,
                                          int frame_end, int col_sz, jobject* cols) {
    auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
    auto obj = helper.convert_handle_to_jobject(_function_context, state);
    LOCAL_REF_GUARD(obj);

    jmethodID window_update = _ctx->ctx->window_update->get_method_id();
    jvalue jvalues[5 + col_sz];
    jvalues[0].l = obj;
    jvalues[1].j = peer_group_start;
    jvalues[2].j = peer_group_end;
    jvalues[3].j = frame_start;
    jvalues[4].j = frame_end;

    for (int i = 0; i < col_sz; ++i) {
        jvalues[i + 5].l = cols[i];
    }

    jobject res = env->CallObjectMethodA(_udaf_handle, window_update, jvalues);
    CHECK_UDF_CALL_EXCEPTION(env, _function_context);
    return res;
}

} // namespace starrocks
