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

#include <memory>
#include <string>

#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "common/status.h"
#include "exprs/function_context.h"
#include "fmt/core.h"
#include "jni.h"
#include "runtime/java/native_method_helper.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "udf/java/java_udf_context.h"
#include "udf/java/java_udf_reflection.h"
#include "udf/java/type_traits.h"

// find a jclass and return a global jclass ref
#define JNI_FIND_CLASS(clazz_name)                                                                 \
    [](JNIEnv* env, const char* name) {                                                            \
        auto clazz = env->FindClass(name);                                                         \
        CHECK(clazz != nullptr) << "not found class" << name << " plz check JDK and jni-packages"; \
        auto g_clazz = env->NewGlobalRef(clazz);                                                   \
        env->DeleteLocalRef(clazz);                                                                \
        return (jclass)g_clazz;                                                                    \
    }(JVMHelper::getInstance().getEnv(), clazz_name)

namespace starrocks {

constexpr const char* CLASS_UDF_HELPER_NAME = "com.starrocks.udf.UDFHelper";

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

#define INIT_STATIC_METHOD(target, clazz, name, signature)   \
    target = env->GetStaticMethodID(clazz, name, signature); \
    CHECK(target != nullptr) << "not found method:" << name << " plz check your jni-packages";

#define INIT_HELPER_METHOD(target, name, signature) INIT_STATIC_METHOD(target, _udf_helper_class, name, signature);

#define SET_METHOD_ID(target, clazz, name, signature)  \
    target = env->GetMethodID(clazz, name, signature); \
    DCHECK(target != nullptr);

StatusOr<jobject> MapMeta::newLocalInstance(jobject keys, jobject values) const {
    JNIEnv* env = JVMHelper::getInstance().getEnv();
    auto res = env->NewObject(immutable_map_class->clazz(), immutable_map_constructor, keys, values);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

JVMFunctionHelper& JVMFunctionHelper::getInstance() {
    JVMHelper::getInstance();
    static JVMFunctionHelper helper;
    return helper;
}

void JVMFunctionHelper::_init() {
    auto* env = JVMHelper::getInstance().getEnv();

    // Register the shared Java native methods before UDFHelper invokes them.
    Status native_method_status = NativeMethodHelper::ensure_registered(env);
    CHECK(native_method_status.ok()) << native_method_status.to_string();

    // init UDF Helper
    std::string name = JVMHelper::to_jni_class_name(CLASS_UDF_HELPER_NAME);
    _udf_helper_class = JNI_FIND_CLASS(name.c_str());
    DCHECK(_udf_helper_class != nullptr);

    INIT_HELPER_METHOD(_create_boxed_array, "createBoxedArray", "(IIZ[Ljava/nio/ByteBuffer;)[Ljava/lang/Object;");
    INIT_HELPER_METHOD(_create_boxed_decimal_array, "createBoxedDecimalArray",
                       "(IIILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)[Ljava/lang/Object;");
    INIT_HELPER_METHOD(_create_boxed_struct_array, "createBoxedStructArray", CREATE_BOXED_STRUCT_SIGNATURE);
    INIT_HELPER_METHOD(_write_result, "writeResult", WRITE_RESULT_SIGNATURE);

    // Cache the UdfTypeDesc class + constructor + field IDs for the BE-side
    // input boxing recursion (which reads children / recordClass per STRUCT slot)
    // and for the per-UDF type desc construction in _build_udf_func_desc.
    {
        std::string td_name = JVMHelper::to_jni_class_name("com.starrocks.udf.UdfTypeDesc");
        _udf_type_desc_class = JNI_FIND_CLASS(td_name.c_str());
        DCHECK(_udf_type_desc_class != nullptr);
        _udf_type_desc_ctor = env->GetMethodID(_udf_type_desc_class, "<init>",
                                               "(I[Lcom/starrocks/udf/UdfTypeDesc;IILjava/lang/Class;)V");
        DCHECK(_udf_type_desc_ctor != nullptr);
        _udf_type_desc_record_class = env->GetFieldID(_udf_type_desc_class, "recordClass", "Ljava/lang/Class;");
        _udf_type_desc_children = env->GetFieldID(_udf_type_desc_class, "children", "[Lcom/starrocks/udf/UdfTypeDesc;");
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

    // init immutable map meta
    auto immutable_map_clazz = JNI_FIND_CLASS("com/starrocks/udf/ImmutableMap");
    DCHECK(immutable_map_clazz != nullptr);
    _map_meta.immutable_map_class = new JVMClass(immutable_map_clazz);
    _map_meta.map_class = &JVMHelper::getInstance().map_class();
    _map_meta.immutable_map_constructor =
            env->GetMethodID(_map_meta.immutable_map_class->clazz(), "<init>", "(Ljava/util/List;Ljava/util/List;)V");

    name = JVMHelper::to_jni_class_name(UDAFStateList::clazz_name);
    jclass loaded_clazz = JNI_FIND_CLASS(name.c_str());
    _function_states_clazz = new JVMClass(loaded_clazz);
}

JVMClass& JVMFunctionHelper::function_state_clazz() {
    return *_function_states_clazz;
}

#define RETURN_ERROR_IF_EXCEPTION(env, errmsg)                                                     \
    if (jthrowable jthr = env->ExceptionOccurred()) {                                              \
        LOCAL_REF_GUARD(jthr);                                                                     \
        std::string msg = fmt::format(errmsg, JVMHelper::getInstance().dumpExceptionString(jthr)); \
        LOG(WARNING) << msg;                                                                       \
        env->ExceptionClear();                                                                     \
        return Status::RuntimeError(msg);                                                          \
    }

Status JVMFunctionHelper::_check_exception_status() {
    auto* env = JVMHelper::getInstance().getEnv();
    RETURN_ERROR_IF_EXCEPTION(env, "exception happened when invoke method: {}");
    return Status::OK();
}

jobject JVMFunctionHelper::create_boxed_array(int type, int num_rows, bool nullable, DirectByteBuffer* buffs, int sz) {
    auto& jvm = JVMHelper::getInstance();
    auto* env = jvm.getEnv();
    jobjectArray input_arr = env->NewObjectArray(sz, jvm.direct_buffer_class(), nullptr);
    RETURN_IF_JNI_EXCEPTION(env, "create_boxed_array: NewObjectArray failed", nullptr);
    LOCAL_REF_GUARD(input_arr);
    for (int i = 0; i < sz; ++i) {
        env->SetObjectArrayElement(input_arr, i, buffs[i].handle());
    }
    jobject res =
            env->CallStaticObjectMethod(_udf_helper_class, _create_boxed_array, type, num_rows, nullable, input_arr);
    RETURN_IF_JNI_EXCEPTION(env, "create_boxed_array", nullptr);
    return res;
}

StatusOr<jobject> JVMFunctionHelper::create_boxed_decimal_array(int type, int scale, int num_rows, jobject null_buff,
                                                                jobject data_buff) {
    auto* env = JVMHelper::getInstance().getEnv();
    jobject res = env->CallStaticObjectMethod(_udf_helper_class, _create_boxed_decimal_array, type, num_rows, scale,
                                              null_buff, data_buff);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "create_boxed_decimal_array");
    return res;
}

StatusOr<jobject> JVMFunctionHelper::create_boxed_struct_array(jclass record_class, int num_rows, jobject null_buff,
                                                               jobject field_arrays) {
    auto* env = JVMHelper::getInstance().getEnv();
    jobject res = env->CallStaticObjectMethod(_udf_helper_class, _create_boxed_struct_array, num_rows, null_buff,
                                              record_class, field_arrays);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "create_boxed_struct_array");
    return res;
}

StatusOr<jobject> JVMFunctionHelper::new_udf_type_desc(jint logical_type, jobjectArray children, jint precision,
                                                       jint scale, jobject record_class) {
    auto* env = JVMHelper::getInstance().getEnv();
    jobject obj = env->NewObject(_udf_type_desc_class, _udf_type_desc_ctor, logical_type, children, precision, scale,
                                 record_class);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "new_udf_type_desc");
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
    auto* env = JVMHelper::getInstance().getEnv();
    env->CallStaticVoidMethod(_udf_helper_class, _write_result, num_rows, result, column_addr, type_desc);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "write_result");
    return Status::OK();
}

jobject JVMFunctionHelper::batch_create_bytebuf(unsigned char* ptr, const uint32_t* offset, int begin, int end) {
    auto* env = JVMHelper::getInstance().getEnv();
    int size = end - begin;
    auto offsets = env->NewIntArray(size + 1);
    RETURN_IF_JNI_EXCEPTION(env, "batch_create_bytebuf: NewIntArray failed", nullptr);
    env->SetIntArrayRegion(offsets, 0, size + 1, (const int32_t*)offset);
    LOCAL_REF_GUARD(offsets);
    auto res = env->CallStaticObjectMethod(_udf_helper_class, _batch_create_bytebuf, ptr, offsets, size);
    RETURN_IF_JNI_EXCEPTION(env, "batch_create_bytebuf", nullptr);
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
    auto& jvm = JVMHelper::getInstance();
    auto* env = jvm.getEnv();
    jobjectArray input_arr = jvm.build_object_array(jvm.object_array_class(), input, cols);
    LOCAL_REF_GUARD(input_arr);
    env->CallStaticVoidMethod(_udf_helper_class, _batch_update, udaf, update, udaf_ctx->states->handle(), states,
                              input_arr);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

void JVMFunctionHelper::batch_update_state(FunctionContext* ctx, jobject udaf, jobject update, jobject* input,
                                           int cols) {
    auto& jvm = JVMHelper::getInstance();
    auto* env = jvm.getEnv();
    jobjectArray input_arr = jvm.build_object_array(jvm.object_array_class(), input, cols);
    LOCAL_REF_GUARD(input_arr);
    env->CallStaticVoidMethod(_udf_helper_class, _batch_update_state, udaf, update, input_arr);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

void JVMFunctionHelper::batch_update_if_not_null(FunctionContext* ctx, jobject udaf, jobject update, jobject states,
                                                 jobject* input, int cols) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    DCHECK(udaf_ctx != nullptr);
    DCHECK(udaf_ctx->states != nullptr);
    auto& jvm = JVMHelper::getInstance();
    auto* env = jvm.getEnv();
    jobjectArray input_arr = jvm.build_object_array(jvm.object_array_class(), input, cols);
    LOCAL_REF_GUARD(input_arr);
    env->CallStaticVoidMethod(_udf_helper_class, _batch_update_if_not_null, udaf, update, udaf_ctx->states->handle(),
                              states, input_arr);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

StatusOr<jobject> JVMFunctionHelper::batch_call(BatchEvaluateStub* stub, jobject* input, int cols, int rows) {
    return stub->batch_evaluate(rows, input, cols);
}

jobject JVMFunctionHelper::batch_call(FunctionContext* ctx, jobject caller, jobject method, jobject* input, int cols,
                                      int rows) {
    auto& jvm = JVMHelper::getInstance();
    auto* env = jvm.getEnv();
    jobjectArray input_arr = jvm.build_object_array(jvm.object_array_class(), input, cols);
    LOCAL_REF_GUARD(input_arr);
    auto res = env->CallStaticObjectMethod(_udf_helper_class, _batch_call, caller, method, rows, input_arr);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
    return res;
}

jobject JVMFunctionHelper::batch_call(FunctionContext* ctx, jobject caller, jobject method, int rows) {
    auto* env = JVMHelper::getInstance().getEnv();
    auto res = env->CallStaticObjectMethod(_udf_helper_class, _batch_call_no_args, caller, method, rows);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
    return res;
}

jobject JVMFunctionHelper::int_batch_call(FunctionContext* ctx, jobject callers, jobject method, int rows) {
    auto* env = JVMHelper::getInstance().getEnv();
    auto res = env->CallStaticObjectMethod(_udf_helper_class, _int_batch_call, callers, method, rows);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
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
    auto* env = JVMHelper::getInstance().getEnv();
    invoke_boxed_result(env, _udf_helper_class, _get_boxed_result, _get_decimal_boxed_result, type, precision, scale,
                        col, jcolumn, rows, error_if_overflow);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return Status::OK();
}

void JVMFunctionHelper::get_result_from_boxed_array(FunctionContext* ctx, int type, Column* col, jobject jcolumn,
                                                    int rows, int precision, int scale, bool error_if_overflow) {
    auto* env = JVMHelper::getInstance().getEnv();
    invoke_boxed_result(env, _udf_helper_class, _get_boxed_result, _get_decimal_boxed_result, type, precision, scale,
                        col, jcolumn, rows, error_if_overflow);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

// convert UDAF ctx to jobject
jobject JVMFunctionHelper::convert_handle_to_jobject(FunctionContext* ctx, int state) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    DCHECK(udaf_ctx != nullptr);
    DCHECK(udaf_ctx->states != nullptr);
    auto* states = udaf_ctx->states.get();
    return states->get_state(ctx, JVMHelper::getInstance().getEnv(), state);
}

jobject JVMFunctionHelper::convert_handles_to_jobjects(FunctionContext* ctx, jobject state_ids) {
    auto* udaf_ctx = get_java_udaf_context(ctx);
    DCHECK(udaf_ctx != nullptr);
    DCHECK(udaf_ctx->states != nullptr);
    auto* states = udaf_ctx->states.get();
    return states->get_state(ctx, JVMHelper::getInstance().getEnv(), state_ids);
}

StatusOr<jobject> JVMFunctionHelper::extract_key_list(jobject map) {
    auto* env = JVMHelper::getInstance().getEnv();
    auto res = env->CallStaticObjectMethod(_udf_helper_class, _extract_keys_from_map, map);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

StatusOr<jobject> JVMFunctionHelper::extract_val_list(jobject map) {
    auto* env = JVMHelper::getInstance().getEnv();
    auto res = env->CallStaticObjectMethod(_udf_helper_class, _extract_values_from_map, map);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

jlong JVMFunctionHelper::unscaled_long(jobject big_decimal, int precision, int scale) {
    return JVMHelper::getInstance().getEnv()->CallStaticLongMethod(
            _udf_helper_class, _bd_unscaled_long, big_decimal, static_cast<jint>(precision), static_cast<jint>(scale));
}

jbyteArray JVMFunctionHelper::unscaled_le_bytes(jobject big_decimal, int precision, int scale, int byte_width) {
    return (jbyteArray)JVMHelper::getInstance().getEnv()->CallStaticObjectMethod(
            _udf_helper_class, _bd_unscaled_le_bytes, big_decimal, static_cast<jint>(precision),
            static_cast<jint>(scale), static_cast<jint>(byte_width));
}

// DateValue stores days as Julian day; LocalDate exposes them as days-since-1970-01-01.
// 2440588 is the Julian day number of the Unix epoch.
static constexpr jlong UNIX_EPOCH_JULIAN_DAYS = 2440588;

jobject JVMFunctionHelper::newLocalDate(int32_t julian) {
    auto* env = JVMHelper::getInstance().getEnv();
    jobject ld = JVMHelper::getInstance().newLocalDateFromEpochDay(static_cast<jlong>(julian) - UNIX_EPOCH_JULIAN_DAYS);
    RETURN_IF_JNI_EXCEPTION(env, "newLocalDate: ofEpochDay failed", nullptr);
    return ld;
}

int32_t JVMFunctionHelper::valLocalDate(jobject obj) {
    jlong epoch_day = JVMHelper::getInstance().valLocalDateToEpochDay(obj);
    return static_cast<int32_t>(epoch_day + UNIX_EPOCH_JULIAN_DAYS);
}

jobject JVMFunctionHelper::newLocalDateTime(int64_t packed_timestamp) {
    auto* env = JVMHelper::getInstance().getEnv();
    jobject ldt = env->CallStaticObjectMethod(_udf_helper_class, _local_datetime_from_packed,
                                              static_cast<jlong>(packed_timestamp));
    RETURN_IF_JNI_EXCEPTION(env, "newLocalDateTime: localDateTimeFromPackedTimestamp failed", nullptr);
    return ldt;
}

int64_t JVMFunctionHelper::valLocalDateTime(jobject obj) {
    return JVMHelper::getInstance().getEnv()->CallStaticLongMethod(_udf_helper_class, _local_datetime_to_packed, obj);
}

void JVMFunctionHelper::clear(DirectByteBuffer* buffer, FunctionContext* ctx) {
    auto& jvm = JVMHelper::getInstance();
    auto* env = jvm.getEnv();
    auto res = env->CallNonvirtualObjectMethod(buffer->handle(), jvm.direct_buffer_class(), jvm.direct_buffer_clear());
    LOCAL_REF_GUARD(res);
    CHECK_UDF_CALL_EXCEPTION(env, ctx);
}

} // namespace starrocks
