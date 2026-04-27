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

#include "base/string/slice.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/function_context.h"
#include "jni.h"
#include "types/logical_type.h"
#include "udf/java/type_traits.h"

// implements by libhdfs
// hadoop-hdfs-native-client/src/main/native/libhdfs/jni_helper.c
// Why do we need to use this function?
// 1. a thread can not attach to more than one virtual machine
// 2. libhdfs depends on this function and does some initialization,
// if the JVM has already created it, it won't create it anymore.
// If we skip this function call will cause libhdfs to miss some initialization operations
extern "C" JNIEnv* getJNIEnv(void);

#define DEFINE_JAVA_PRIM_TYPE(TYPE) \
    jclass _class_##TYPE;           \
    jmethodID _value_of_##TYPE;     \
    jmethodID _val_##TYPE;

#define DECLARE_NEW_BOX(PRIM_CLAZZ, TYPE, CLAZZ) \
    jobject new##CLAZZ(TYPE value);              \
    TYPE val##TYPE(jobject obj);                 \
    jclass TYPE##_class() { return _class_##PRIM_CLAZZ; }

namespace starrocks {
class DirectByteBuffer;
class AggBatchCallStub;
class BatchEvaluateStub;
class JVMClass;

struct ListMeta {
    // List class
    JVMClass* list_class;
    JVMClass* array_list_class;

    // List method
    jmethodID list_get;
    jmethodID list_size;
    jmethodID list_add;
};

struct MapMeta {
    JVMClass* map_class;
    JVMClass* immutable_map_class;
    jmethodID immutable_map_constructor;
    StatusOr<jobject> newLocalInstance(jobject keys, jobject values) const;
};

// Restrictions on use:
// can only be used in pthread, not in bthread
// thread local helper
class JVMFunctionHelper {
public:
    static JVMFunctionHelper& getInstance();
    static std::pair<JNIEnv*, JVMFunctionHelper&> getInstanceWithEnv();
    JVMFunctionHelper(const JVMFunctionHelper&) = delete;
    // get env
    JNIEnv* getEnv() { return _env; }
    // Arrays.toString()
    std::string array_to_string(jobject object);
    // Object::toString()
    bool equals(jobject obj1, jobject obj2);
    std::string to_string(jobject obj);
    std::string to_cxx_string(jstring str);
    std::string dumpExceptionString(jthrowable throwable);
    jmethodID getToStringMethod(jclass clazz);
    StatusOr<jstring> to_jstring(const std::string& str);
    jmethodID getMethod(jclass clazz, const std::string& method, const std::string& sig);
    jmethodID getStaticMethod(jclass clazz, const std::string& method, const std::string& sig);
    // create a object array
    jobject create_array(int sz);
    // convert column data to Java Object Array
    jobject create_boxed_array(int type, int num_rows, bool nullable, DirectByteBuffer* buffs, int sz);
    // Convert a DECIMAL column to a BigDecimal[] Java array. `type` is the LogicalType
    // (TYPE_DECIMAL32/64/128) and `scale` is the DECIMAL scale. `null_buff` may be null
    // for a non-nullable column; `data_buff` is the raw unscaled integer buffer
    // (int32/int64/int128 little-endian).
    StatusOr<jobject> create_boxed_decimal_array(int type, int scale, int num_rows, jobject null_buff,
                                                 jobject data_buff);
    const std::unordered_map<int, jmethodID>& method_map() const { return _method_map; }

    template <class... Args>
    StatusOr<jobject> invoke_static_method(jmethodID method, Args&&... args) {
        jobject res = _env->CallStaticObjectMethod(_udf_helper_class, method, args...);
        RETURN_IF_ERROR(_check_exception_status());
        return res;
    }
    // create object array with the same elements
    jobject create_object_array(jobject o, int num_rows);
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

    // type: LogicalType
    // col: result column
    // jcolumn: Integer[]/String[]
    void get_result_from_boxed_array(FunctionContext* ctx, int type, Column* col, jobject jcolumn, int rows);

    Status get_result_from_boxed_array(int type, Column* col, jobject jcolumn, int rows);

    // DECIMAL-aware result conversion. Used when the UDF return type is DECIMAL32/64/128/256
    // so that the BigDecimal[] produced on the Java side can be materialized into a native
    // DECIMAL column (int32/int64/int128/int256 unscaled) using `scale` for rounding.
    // `precision` is the declared DECIMAL precision used for overflow detection; when a
    // value does not fit, `error_if_overflow` decides between raising an ArithmeticException
    // (REPORT_ERROR) or nulling the row (OUTPUT_NULL).
    Status get_decimal_result_from_boxed_array(int type, int precision, int scale, Column* col, jobject jcolumn,
                                               int rows, bool error_if_overflow);

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

    DECLARE_NEW_BOX(boolean, uint8_t, Boolean)
    DECLARE_NEW_BOX(byte, int8_t, Byte)
    DECLARE_NEW_BOX(short, int16_t, Short)
    DECLARE_NEW_BOX(int, int32_t, Integer)
    DECLARE_NEW_BOX(long, int64_t, Long)
    DECLARE_NEW_BOX(float, float, Float)
    DECLARE_NEW_BOX(double, double, Double)

    const ListMeta& list_meta() const { return _list_meta; }
    const MapMeta& map_meta() const { return _map_meta; }

    StatusOr<jobject> extract_key_list(jobject map);
    StatusOr<jobject> extract_val_list(jobject map);

    jobject newString(const char* data, size_t size);

    // Construct a new java.math.BigDecimal from its string representation.
    // Returns a local ref (nullptr on failure, error pushed via JNI exception).
    jobject newBigDecimal(const std::string& s);

    // Fast path: build a BigDecimal via BigDecimal.valueOf(unscaledVal, scale).
    // Avoids the string-parsing cost used by the (String) constructor and is suitable
    // for DECIMAL32 / DECIMAL64 whose unscaled value fits in int64_t.
    jobject newBigDecimal(int64_t unscaled, int scale);

    Slice sliceVal(jstring jstr, std::string* buffer);
    jclass string_clazz() { return _string_class; }
    jclass big_decimal_class() { return _big_decimal_class; }
    // replace '.' as '/'
    // eg: java.lang.Integer -> java/lang/Integer
    static std::string to_jni_class_name(const std::string& name);

    // reset Buffer set read/write position to zero
    void clear(DirectByteBuffer* buffer, FunctionContext* ctx);

    jclass object_class() { return _object_class; }

    JVMClass& function_state_clazz();

private:
    JVMFunctionHelper() { _init(); };
    void _init();
    // pack input array to java object array
    jobjectArray _build_object_array(jclass clazz, jobject* arr, int sz);

    Status _check_exception_status();

private:
    inline static thread_local JNIEnv* _env;

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

    jmethodID _string_construct_with_bytes;
    jmethodID _big_decimal_ctor_string;
    jmethodID _big_decimal_value_of_ll;

    ListMeta _list_meta;
    MapMeta _map_meta;
    jmethodID _extract_keys_from_map;
    jmethodID _extract_values_from_map;

    jobject _utf8_charsets;

    jclass _udf_helper_class;
    jmethodID _create_boxed_array;
    jmethodID _create_boxed_decimal_array;
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
    jclass _direct_buffer_class;
    jmethodID _direct_buffer_clear;

    JVMClass* _function_states_clazz = nullptr;
};

// local object reference guard.
// The objects inside are automatically call DeleteLocalRef in the life object.
#define LOCAL_REF_GUARD(lref)                                                \
    DeferOp VARNAME_LINENUM(guard)([&lref]() {                               \
        if (lref) {                                                          \
            JVMFunctionHelper::getInstance().getEnv()->DeleteLocalRef(lref); \
            lref = nullptr;                                                  \
        }                                                                    \
    })

#define LOCAL_REF_GUARD_ENV(env, lref)              \
    DeferOp VARNAME_LINENUM(guard)([&lref, env]() { \
        if (lref) {                                 \
            env->DeleteLocalRef(lref);              \
            lref = nullptr;                         \
        }                                           \
    })

// check JNI Exception and set error in FunctionContext
#define CHECK_UDF_CALL_EXCEPTION(env, ctx)                                         \
    if (auto e = env->ExceptionOccurred()) {                                       \
        LOCAL_REF_GUARD(e);                                                        \
        std::string msg = JVMFunctionHelper::getInstance().dumpExceptionString(e); \
        LOG(WARNING) << "Exception: " << msg;                                      \
        ctx->set_error(msg.c_str());                                               \
        env->ExceptionClear();                                                     \
    }

#define RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, prefix)                     \
    if (auto e = env->ExceptionOccurred()) {                                       \
        LOCAL_REF_GUARD(e);                                                        \
        std::string err = JVMFunctionHelper::getInstance().dumpExceptionString(e); \
        env->ExceptionClear();                                                     \
        return Status::InternalError(fmt::format("{}, {}", prefix, err));          \
    }

#define RETURN_ERROR_IF_JNI_EXCEPTION(env) RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "JNI Exception")

#define RETURN_IF_JNI_EXCEPTION(env, errmsg, ret)                                                                   \
    if (jthrowable jthr = env->ExceptionOccurred()) {                                                               \
        LOCAL_REF_GUARD(jthr);                                                                                      \
        std::string msg = fmt::format("{},{}", errmsg, JVMFunctionHelper::getInstance().dumpExceptionString(jthr)); \
        LOG(WARNING) << msg;                                                                                        \
        env->ExceptionClear();                                                                                      \
        return ret;                                                                                                 \
    }

// Used for UDAF serialization and deserialization,
// providing a C++ memory space for Java to access.
// DirectByteBuffer does not hold ownership of this memory space
// Handle will be freed during destructuring,
// but no operations will be done on this memory space
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

// A global ref of the guard, handle can be shared across threads
class JavaGlobalRef {
public:
    JavaGlobalRef(jobject handle) : _handle(handle) {}
    ~JavaGlobalRef();
    JavaGlobalRef(const JavaGlobalRef&) = delete;

    JavaGlobalRef(JavaGlobalRef&& other) noexcept {
        _handle = other._handle;
        other._handle = nullptr;
    }

    JavaGlobalRef& operator=(JavaGlobalRef&& other) noexcept {
        JavaGlobalRef tmp(std::move(other));
        std::swap(this->_handle, tmp._handle);
        return *this;
    }

    jobject handle() const { return _handle; }

    jobject& handle() { return _handle; }

    void clear();

private:
    jobject _handle;
};

// A Class object created from the ClassLoader that can be accessed by multiple threads
class JVMClass {
public:
    JVMClass(jobject clazz) : _clazz(clazz) {}
    JVMClass(const JVMClass&) = delete;

    JVMClass& operator=(const JVMClass&&) = delete;
    JVMClass& operator=(const JVMClass& other) = delete;

    JVMClass(JVMClass&& other) noexcept : _clazz(nullptr) { _clazz = std::move(other._clazz); }

    JVMClass& operator=(JVMClass&& other) noexcept {
        JVMClass tmp(std::move(other));
        std::swap(this->_clazz, tmp._clazz);
        return *this;
    }

    jclass clazz() const { return (jclass)_clazz.handle(); }

    // Create a new instance using the default constructor
    StatusOr<JavaGlobalRef> newInstance() const;
    StatusOr<jobject> newLocalInstance() const;

private:
    JavaGlobalRef _clazz;
};

class AggBatchCallStub {
public:
    static inline const char* stub_clazz_name = "com.starrocks.udf.gen.CallStub";
    static inline const char* batch_update_method_name = "batchCallV";

    AggBatchCallStub(FunctionContext* ctx, jobject caller, JVMClass&& clazz, JavaGlobalRef&& method)
            : _ctx(ctx), _caller(caller), _stub_clazz(std::move(clazz)), _stub_method(std::move(method)) {}

    FunctionContext* ctx() { return _ctx; }

    void batch_update_single(int num_rows, jobject state, jobject* input, int cols);

private:
    FunctionContext* _ctx;
    // UDAF object handle, owned by JavaUDAFUniqueContext
    jobject _caller;
    JVMClass _stub_clazz;
    JavaGlobalRef _stub_method;
};

class BatchEvaluateStub {
public:
    static inline const char* stub_clazz_name = "com.starrocks.udf.gen.CallStub";
    static inline const char* batch_evaluate_method_name = "batchCallV";

    BatchEvaluateStub(jobject caller, JVMClass&& clazz, JavaGlobalRef&& method)
            : _caller(caller), _stub_clazz(std::move(clazz)), _stub_method(std::move(method)) {}

    StatusOr<jobject> batch_evaluate(int num_rows, jobject* input, int cols);

private:
    jobject _caller;
    JVMClass _stub_clazz;
    JavaGlobalRef _stub_method;
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

// UDAF State Lists
// mapping a java object as a int index
// use get method to
// TODO: implement a Java binder to avoid using this class
class UDAFStateList {
public:
    static inline const char* clazz_name = "com.starrocks.udf.FunctionStates";
    UDAFStateList(JavaGlobalRef&& handle, JavaGlobalRef&& get, JavaGlobalRef&& batch_get, JavaGlobalRef&& add,
                  JavaGlobalRef&& remove, JavaGlobalRef&& clear);

    jobject handle() { return _handle.handle(); }

    // get state with index state
    jobject get_state(FunctionContext* ctx, JNIEnv* env, int state);

    // batch get states
    jobject get_state(FunctionContext* ctx, JNIEnv* env, jobject state_ids);

    // add a state to StateList
    int add_state(FunctionContext* ctx, JNIEnv* env, jobject state);

    // remove a state from StateList
    void remove(FunctionContext* ctx, JNIEnv* env, int state);

    // clear all state in StateList
    void clear(FunctionContext* ctx, JNIEnv* env);

private:
    JavaGlobalRef _handle;
    JavaGlobalRef _get_method;
    JavaGlobalRef _batch_get_method;
    JavaGlobalRef _add_method;
    JavaGlobalRef _remove_method;
    JavaGlobalRef _clear_method;
    jmethodID _get_method_id;
    jmethodID _batch_get_method_id;
    jmethodID _add_method_id;
    jmethodID _remove_method_id;
    jmethodID _clear_method_id;
};

// For loading UDF Class
// Not thread safe
class ClassLoader {
public:
    static const inline int BATCH_SINGLE_UPDATE = 1;
    static const inline int BATCH_EVALUATE = 2;
    // Handle
    ClassLoader(std::string path) : _path(std::move(path)) {}
    ~ClassLoader();

    ClassLoader& operator=(const ClassLoader& other) = delete;
    ClassLoader(const ClassLoader&) = delete;
    // get class
    StatusOr<JVMClass> getClass(const std::string& className);
    // get batch call stub
    // numActualVarArgs: actual number of varargs input columns; only meaningful when the method uses varargs
    StatusOr<JVMClass> genCallStub(const std::string& stubClassName, jclass clazz, jobject method, int type,
                                   int numActualVarArgs = 0);

    Status init();

private:
    std::string _path;
    jmethodID _get_class = nullptr;
    jmethodID _get_call_stub = nullptr;
    JavaGlobalRef _handle = nullptr;
    JavaGlobalRef _clazz = nullptr;
};

struct MethodTypeDescriptor {
    LogicalType type;
    bool is_box;
    bool is_array = false;
};

struct JavaMethodDescriptor {
    std::string signature; // function signature
    std::string name;      // function name
    std::vector<MethodTypeDescriptor> method_desc;
    JavaGlobalRef method = nullptr;
    // thread safe
    jmethodID get_method_id() const;
};

// Used to get function signatures
class ClassAnalyzer {
public:
    ClassAnalyzer() = default;
    ~ClassAnalyzer() = default;

    // Strip generic type parameters from a JNI method signature.
    // e.g. "(Ljava/util/List<java/lang/String>;)V" -> "(Ljava/util/List;)V"
    static void strip_jni_generic_types(std::string* sign);

    Status has_method(jclass clazz, const std::string& method, bool* has);
    Status get_signature(jclass clazz, const std::string& method, std::string* sign);
    Status get_method_desc(const std::string& sign, std::vector<MethodTypeDescriptor>* desc);
    StatusOr<jobject> get_method_object(jclass clazz, const std::string& method_name);
    Status get_udaf_method_desc(const std::string& sign, std::vector<MethodTypeDescriptor>* desc);
};

struct JavaUDFContext {
    JavaUDFContext() = default;
    ~JavaUDFContext();

    std::unique_ptr<ClassLoader> udf_classloader;
    std::unique_ptr<ClassAnalyzer> analyzer;
    std::unique_ptr<BatchEvaluateStub> call_stub;

    JVMClass udf_class = nullptr;
    JavaGlobalRef udf_handle = nullptr;

    // Java Method
    std::unique_ptr<JavaMethodDescriptor> prepare;
    std::unique_ptr<JavaMethodDescriptor> evaluate;
    std::unique_ptr<JavaMethodDescriptor> close;
};

// Shareable, cacheable UDAF class-level context.
// Contains class references and method descriptors — no per-aggregation state.
// Cached via UserFunctionCache::load_cacheable_java_udf.
struct JavaUDAFSharedContext {
    std::unique_ptr<ClassLoader> udf_classloader;

    JVMClass udaf_class = nullptr;
    JVMClass udaf_state_class = nullptr;

    std::unique_ptr<JavaMethodDescriptor> create;
    std::unique_ptr<JavaMethodDescriptor> destory;
    std::unique_ptr<JavaMethodDescriptor> update;
    std::unique_ptr<JavaMethodDescriptor> merge;
    std::unique_ptr<JavaMethodDescriptor> finalize;
    std::unique_ptr<JavaMethodDescriptor> serialize;
    std::unique_ptr<JavaMethodDescriptor> serialize_size;

    // Window function methods (optional)
    std::unique_ptr<JavaMethodDescriptor> reset;
    std::unique_ptr<JavaMethodDescriptor> window_update;
    std::unique_ptr<JavaMethodDescriptor> get_values;

    // Generated stub class/method — used to create a per-aggregator AggBatchCallStub
    JVMClass update_stub_clazz = nullptr;
    JavaGlobalRef update_stub_method = nullptr;

    // FunctionStates method objects — looked up once, cloned per aggregator instance
    JavaGlobalRef states_get_method = nullptr;
    JavaGlobalRef states_batch_get_method = nullptr;
    JavaGlobalRef states_add_method = nullptr;
    JavaGlobalRef states_remove_method = nullptr;
    JavaGlobalRef states_clear_method = nullptr;
};

// Per-aggregator UDAF context stored as FunctionContext::THREAD_LOCAL.
// Holds a reference to the shared class-level JavaUDAFSharedContext plus
// mutable per-aggregation state.
struct JavaUDAFUniqueContext;

class UDAFFunction {
public:
    UDAFFunction(jobject udaf_handle, FunctionContext* function_ctx, JavaUDAFUniqueContext* ctx)
            : _udaf_handle(udaf_handle), _function_context(function_ctx), _ctx(ctx) {}
    // create a new state for UDAF
    int create();
    // destroy state
    void destroy(int state);
    // UDAF Update Function
    void update(jvalue* val);
    // UDAF merge
    void merge(int state, jobject buffer);
    void serialize(int state, jobject buffer);
    // UDAF State serialize_size
    int serialize_size(int state);
    // UDAF finalize
    jvalue finalize(int state);

    // WindowFunction reset
    void reset(int state);

    // WindowFunction updateBatch
    jobject window_update_batch(int state, int peer_group_start, int peer_group_end, int frame_start, int frame_end,
                                int col_sz, jobject* cols);

private:
    jobject _convert_to_jobject(int state);

    // not owned udaf function handle
    jobject _udaf_handle;
    FunctionContext* _function_context;
    JavaUDAFUniqueContext* _ctx;
};

struct JavaUDAFUniqueContext {
    // Shared, possibly cached class-level context
    std::shared_ptr<JavaUDAFSharedContext> ctx;

    // Per-aggregator UDAF object instance and its batch-update stub
    JavaGlobalRef handle = nullptr;
    std::unique_ptr<AggBatchCallStub> update_batch_call_stub;

    // Per-aggregator mutable state
    std::unique_ptr<UDAFStateList> states;
    std::unique_ptr<UDAFFunction> _func;
    std::unique_ptr<DirectByteBuffer> buffer;
    std::vector<uint8_t> buffer_data;
};

// Java UDAF lifecycle management based on FunctionContext::THREAD_LOCAL state.
JavaUDAFUniqueContext* get_java_udaf_context(FunctionContext* ctx);
void attach_java_udaf_context(FunctionContext* ctx, std::unique_ptr<JavaUDAFUniqueContext> udaf_ctx);
void clear_java_udaf_states(FunctionContext* ctx);
void destroy_java_udaf_context(FunctionContext* ctx);

// Check whether java runtime can work
Status detect_java_runtime();

} // namespace starrocks
