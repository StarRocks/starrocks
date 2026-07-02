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
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "common/statusor.h"
#include "exprs/function_context.h"
#include "jni.h"
#include "runtime/java/jvm_class.h"
#include "runtime/java/jvm_helper.h"
#include "types/logical_type.h"
#include "udf/java/type_traits.h"

namespace starrocks {
class DirectByteBuffer;
class AggBatchCallStub;
class BatchEvaluateStub;
class JavaUdfClassLoader;
class JavaUdfClassAnalyzer;
struct JavaUdfMethodDescriptor;

struct MapMeta {
    JVMClass* map_class;
    JVMClass* immutable_map_class;
    jmethodID immutable_map_constructor;
    StatusOr<jobject> newLocalInstance(jobject keys, jobject values) const;
};

// StarRocks UDF-specific JNI helper. Generic JDK/JNI utilities and the thread-local
// JNIEnv live in JVMHelper; this class caches only UDF helper classes and methods.
class JVMFunctionHelper {
public:
    static JVMFunctionHelper& getInstance();
    JVMFunctionHelper(const JVMFunctionHelper&) = delete;

    // convert column data to Java Object Array
    jobject create_boxed_array(int type, int num_rows, bool nullable, DirectByteBuffer* buffs, int sz);
    // Convert a DECIMAL column to a BigDecimal[] Java array. `type` is the LogicalType
    // (TYPE_DECIMAL32/64/128) and `scale` is the DECIMAL scale. `null_buff` may be null
    // for a non-nullable column; `data_buff` is the raw unscaled integer buffer
    // (int32/int64/int128 little-endian).
    StatusOr<jobject> create_boxed_decimal_array(int type, int scale, int num_rows, jobject null_buff,
                                                 jobject data_buff);

    // Materialize a STRUCT column on the Java side as `record_class[num_rows]`.
    // `record_class` is the formal java.lang.Class declared in the UDF method
    // signature; the FE analyzer guarantees its components match the SQL fields.
    // `null_buff` is a ByteBuffer over the parent NullableColumn's null bitmap
    // (may be null for non-nullable columns). `field_arrays` is a jobjectArray
    // (Object[]) of length numFields where each entry is the already-boxed
    // Object[numRows] for that subfield column.
    StatusOr<jobject> create_boxed_struct_array(jclass record_class, int num_rows, jobject null_buff,
                                                jobject field_arrays);

    // Extract record components into per-field boxed Object[]s and write the parent
    // NullableColumn's null bitmap. The BE drives per-subfield writes after this call,
    // recursing back through this method for STRUCT subfields and dispatching DECIMAL
    // / scalar subfields through the existing per-type result writers. Returning the
    // per-field arrays (rather than threading the SQL type tree across the JNI
    // boundary) keeps DECIMAL precision/scale and nested record-class lookups on the
    // C++ side where they already live.
    //
    // Returns a jobjectArray of length `sub_field_types.size()`; each element is a
    // typed array of length `num_rows` (Integer[]/String[]/... for scalar subfields,
    // Object[] for STRUCT subfields, which the BE then re-feeds into this method).
    // Unified output dispatcher: drain a UDF result jobject (records, lists, maps,
    // scalars) into a native column tree using the SQL type info encoded in
    // `type_desc` (a com.starrocks.udf.UdfTypeDesc). The BE constructs the desc
    // once per UDF and just calls this one entry point per query batch.
    Status write_result(jobject result, int num_rows, jlong column_addr, jobject type_desc, bool error_if_overflow);

    // Cached field IDs for com.starrocks.udf.UdfTypeDesc, used by the BE-side input
    // boxing recursion to read recordClass / children without repeated FindClass
    // / GetFieldID lookups.
    jclass udf_type_desc_class() const { return _udf_type_desc_class; }
    jfieldID udf_type_desc_record_class_field() const { return _udf_type_desc_record_class; }
    jfieldID udf_type_desc_children_field() const { return _udf_type_desc_children; }

    // Construct a UdfTypeDesc Java object via the public constructor. Used by the
    // UDF context builder to materialize the per-arg / per-return type tree from BE.
    StatusOr<jobject> new_udf_type_desc(jint logical_type, jobjectArray children, jint precision, jint scale,
                                        jobject record_class);

    const std::unordered_map<int, jmethodID>& method_map() const { return _method_map; }

    template <class... Args>
    StatusOr<jobject> invoke_static_method(jmethodID method, Args&&... args) {
        auto* env = JVMHelper::getInstance().getEnv();
        jobject res = env->CallStaticObjectMethod(_udf_helper_class, method, args...);
        RETURN_IF_ERROR(_check_exception_status());
        return res;
    }

    jobject batch_create_bytebuf(unsigned char* ptr, const uint32_t* offset, int begin, int end);

    // batch update single
    void batch_update_single(AggBatchCallStub* stub, int state, jobject* input, int cols, int rows);

    // batch update input: state col1 col2
    void batch_update(FunctionContext* ctx, jobject udaf, jobject update, jobject states, jobject* input, int cols);

    // batch update if state is not null
    // input: state col1 col2
    void batch_update_if_not_null(FunctionContext* ctx, jobject udaf, jobject update, jobject states, jobject* input,
                                  int cols);

    // only used for AGG streaming
    void batch_update_state(FunctionContext* ctx, jobject udaf, jobject update, jobject* input, int cols);

    // batch call evalute by callstub
    StatusOr<jobject> batch_call(BatchEvaluateStub* stub, jobject* input, int cols, int rows);
    // batch call method by reflect
    jobject batch_call(FunctionContext* ctx, jobject caller, jobject method, jobject* input, int cols, int rows);
    // batch call no-args function by reflect
    jobject batch_call(FunctionContext* ctx, jobject caller, jobject method, int rows);
    // batch call int()
    // callers should be Object[]
    // return: jobject int[]
    jobject int_batch_call(FunctionContext* ctx, jobject callers, jobject method, int rows);

    // Drain a UDF result jobject (Object[] of boxed values) into the native column.
    // For DECIMAL types, supply precision/scale and the overflow policy
    // (REPORT_ERROR vs OUTPUT_NULL); the helper rescales to (precision, scale) and
    // raises ArithmeticException on overflow according to `error_if_overflow`.
    // For non-DECIMAL types, the trailing arguments are ignored.
    Status get_result_from_boxed_array(int type, Column* col, jobject jcolumn, int rows, int precision = 0,
                                       int scale = 0, bool error_if_overflow = true);

    // UDAF variant: same dispatch but reports JNI exceptions through FunctionContext::set_error
    // instead of returning Status. UDAF return types are restricted to scalar/decimal so the
    // dispatch matches the unified scalar/decimal logic above.
    void get_result_from_boxed_array(FunctionContext* ctx, int type, Column* col, jobject jcolumn, int rows,
                                     int precision = 0, int scale = 0, bool error_if_overflow = true);

    // Per-row helpers used by the BE for single-row DECIMAL writes. Both delegate the
    // setScale + range check + unscaled extraction to UDFHelper (Java side) and surface
    // overflow as a JNI exception which the caller is expected to clear.
    //   unscaled_long       - DECIMAL32/64: returns BigDecimal -> long unscaled value.
    //   unscaled_le_bytes   - DECIMAL128/256: returns `byte_width` LE sign-extended bytes;
    //                          caller must DeleteLocalRef on the returned jbyteArray.
    jlong unscaled_long(jobject big_decimal, int precision, int scale);
    jbyteArray unscaled_le_bytes(jobject big_decimal, int precision, int scale, int byte_width);

    // convert int handle to jobject
    // return a local ref
    jobject convert_handle_to_jobject(FunctionContext* ctx, int state);

    // convert handle list to jobject array (Object[])
    jobject convert_handles_to_jobjects(FunctionContext* ctx, jobject state_ids);

    const MapMeta& map_meta() const { return _map_meta; }

    StatusOr<jobject> extract_key_list(jobject map);
    StatusOr<jobject> extract_val_list(jobject map);

    // Box/unbox a StarRocks DateValue (int32 Julian day) as a java.time.LocalDate.
    // Round-trips StarRocks's internal Julian-day encoding.
    jobject newLocalDate(int32_t julian);
    int32_t valLocalDate(jobject obj);

    // Box/unbox a StarRocks TimestampValue (packed int64: julian << 40 | usOfDay)
    // as a java.time.LocalDateTime.
    jobject newLocalDateTime(int64_t packed_timestamp);
    int64_t valLocalDateTime(jobject obj);

    // reset Buffer set read/write position to zero
    void clear(DirectByteBuffer* buffer, FunctionContext* ctx);

    JVMClass& function_state_clazz();

private:
    JVMFunctionHelper() { _init(); };
    void _init();
    Status _check_exception_status();

private:
    // UDFHelper.localDateTimeFromPackedTimestamp(long) / packedTimestampFromLocalDateTime(LocalDateTime)
    jmethodID _local_datetime_from_packed;
    jmethodID _local_datetime_to_packed;

    MapMeta _map_meta;
    jmethodID _extract_keys_from_map;
    jmethodID _extract_values_from_map;

    jclass _udf_helper_class;
    jclass _udf_type_desc_class;
    jmethodID _udf_type_desc_ctor;
    jfieldID _udf_type_desc_record_class;
    jfieldID _udf_type_desc_children;
    jmethodID _create_boxed_array;
    jmethodID _create_boxed_decimal_array;
    jmethodID _create_boxed_struct_array;
    jmethodID _write_result;
    jmethodID _get_decimal_boxed_result;
    jmethodID _bd_unscaled_long;
    jmethodID _bd_unscaled_le_bytes;
    std::unordered_map<int, jmethodID> _method_map;
    jmethodID _batch_update;
    jmethodID _batch_update_if_not_null;
    jmethodID _batch_update_state;
    jmethodID _batch_create_bytebuf;
    jmethodID _batch_call;
    jmethodID _batch_call_no_args;
    jmethodID _int_batch_call;
    jmethodID _get_boxed_result;

    JVMClass* _function_states_clazz = nullptr;
};

// check JNI Exception and set error in FunctionContext
#define CHECK_UDF_CALL_EXCEPTION(env, ctx)                                 \
    if (auto e = env->ExceptionOccurred()) {                               \
        LOCAL_REF_GUARD(e);                                                \
        std::string msg = JVMHelper::getInstance().dumpExceptionString(e); \
        LOG(WARNING) << "Exception: " << msg;                              \
        ctx->set_error(msg.c_str());                                       \
        env->ExceptionClear();                                             \
    }

} // namespace starrocks
