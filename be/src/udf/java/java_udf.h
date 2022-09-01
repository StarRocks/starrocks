// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include <memory>

#include "common/status.h"
#include "common/statusor.h"
#include "jni.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"

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

#define DECLARE_NEW_BOX(TYPE, CLAZZ) \
    jobject new##CLAZZ(TYPE value);  \
    TYPE val##TYPE(jobject obj);

namespace starrocks::vectorized {
class DirectByteBuffer;
// Restrictions on use:
// can only be used in pthread, not in bthread
// thread local helper
class JVMFunctionHelper {
public:
    static JVMFunctionHelper& getInstance();
    JVMFunctionHelper(const JVMFunctionHelper&) = delete;
    // get env
    JNIEnv* getEnv() { return _env; }
    // Arrays.toString()
    std::string array_to_string(jobject object);
    // Object::toString()
    std::string to_string(jobject obj);
    std::string to_cxx_string(jstring str);
    std::string dumpExceptionString(jthrowable throwable);
    jmethodID getToStringMethod(jclass clazz);
    jstring to_jstring(const std::string& str);
    jmethodID getMethod(jclass clazz, const std::string& method, const std::string& sig);
    jmethodID getStaticMethod(jclass clazz, const std::string& method, const std::string& sig);
    // create a object array
    jobject create_array(int sz);
    // convert column data to Java Object Array
    jobject create_boxed_array(int type, int num_rows, bool nullable, DirectByteBuffer* buffs, int sz);
    // create object array with the same elements
    jobject create_object_array(jobject o, int num_rows);
    // batch update input: col1 col2
    void batch_update_single(FunctionContext* ctx, jobject udaf, jobject update, jobject state, jobject* input,
                             int cols);
    // batch update input: state col1 col2
    void batch_update(FunctionContext* ctx, jobject udaf, jobject update, jobject* input, int cols);

    // batch call evalute
    jobject batch_call(FunctionContext* ctx, jobject udf, jobject evaluate, jobject* input, int cols, int rows);
    // batch call no-args function
    jobject batch_call(FunctionContext* ctx, jobject caller, jobject method, int rows);
    // batch call int()
    // callers should be Object[]
    // return: jobject int[]
    jobject int_batch_call(FunctionContext* ctx, jobject callers, jobject method, int rows);

    // type: PrimitiveType
    // col: result column
    // jcolumn: Integer[]/String[]
    void get_result_from_boxed_array(FunctionContext* ctx, int type, Column* col, jobject jcolumn, int rows);

    DECLARE_NEW_BOX(uint8_t, Boolean)
    DECLARE_NEW_BOX(int8_t, Byte)
    DECLARE_NEW_BOX(int16_t, Short)
    DECLARE_NEW_BOX(int32_t, Integer)
    DECLARE_NEW_BOX(int64_t, Long)
    DECLARE_NEW_BOX(float, Float)
    DECLARE_NEW_BOX(double, Double)

    jobject newString(const char* data, size_t size);

    Slice sliceVal(jstring jstr);
    size_t string_length(jstring jstr);
    Slice sliceVal(jstring jstr, std::string* buffer);
    // replace '.' as '/'
    // eg: java.lang.Integer -> java/lang/Integer
    static std::string to_jni_class_name(const std::string& name);

    // reset Buffer set read/write position to zero
    void clear(DirectByteBuffer* buffer, FunctionContext* ctx);

    jclass object_class() { return _object_class; }

    // check JNI Exception and set error in FunctionContext
    static void check_call_exception(JNIEnv* env, FunctionContext* ctx);

private:
    JVMFunctionHelper(JNIEnv* env) : _env(env) {}
    void _init();
    void _add_class_path(const std::string& path);
    // pack input array to java object array
    jobjectArray _build_object_array(jclass clazz, jobject* arr, int sz);

private:
    JNIEnv* _env;

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
    jclass _throwable_class;
    jclass _jarrays_class;

    jmethodID _string_construct_with_bytes;

    jobject _utf8_charsets;

    jclass _udf_helper_class;
    jmethodID _create_boxed_array;
    jmethodID _batch_update_single;
    jmethodID _batch_update;
    jmethodID _batch_call;
    jmethodID _batch_call_no_args;
    jmethodID _int_batch_call;
    jmethodID _get_boxed_result;
    jclass _direct_buffer_class;
    jmethodID _direct_buffer_clear;
};

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

    DirectByteBuffer(DirectByteBuffer&& other) {
        _handle = other._handle;
        _data = other._data;
        _capacity = other._capacity;

        other._handle = nullptr;
        other._data = nullptr;
        other._capacity = 0;
    }

    DirectByteBuffer& operator=(DirectByteBuffer&& other) {
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
    JavaGlobalRef(jobject&& handle) : _handle(std::move(handle)) {}
    ~JavaGlobalRef();
    JavaGlobalRef(const JavaGlobalRef&) = delete;

    JavaGlobalRef(JavaGlobalRef&& other) {
        _handle = other._handle;
        other._handle = nullptr;
    }

    JavaGlobalRef& operator=(JavaGlobalRef&& other) {
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
    JVMClass(jobject&& clazz) : _clazz(std::move(clazz)) {}
    JVMClass(const JVMClass&) = delete;

    JVMClass& operator=(const JVMClass&&) = delete;
    JVMClass& operator=(const JVMClass& other) = delete;

    JVMClass(JVMClass&& other) : _clazz(nullptr) { _clazz = std::move(other._clazz); }

    JVMClass& operator=(JVMClass&& other) {
        JVMClass tmp(std::move(other));
        std::swap(this->_clazz, tmp._clazz);
        return *this;
    }

    jclass clazz() const { return (jclass)_clazz.handle(); }

    // Create a new instance using the default constructor
    StatusOr<JavaGlobalRef> newInstance() const;

private:
    JavaGlobalRef _clazz;
};

// For loading UDF Class
// Not thread safe
class ClassLoader {
public:
    // Handle
    ClassLoader(std::string path) : _path(std::move(path)) {}
    ~ClassLoader();

    ClassLoader& operator=(const ClassLoader& other) = delete;
    ClassLoader(const ClassLoader&) = delete;
    // get class
    StatusOr<JVMClass> getClass(const std::string& className);
    Status init();

private:
    std::string _path;
    jmethodID _get_class = nullptr;
    JavaGlobalRef _handle = nullptr;
    JavaGlobalRef _clazz = nullptr;
};

struct MethodTypeDescriptor {
    PrimitiveType type;
    bool is_box;
    bool is_array;
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
    JVMClass udf_class = nullptr;
    JavaGlobalRef udf_handle = nullptr;

    // Java Method
    std::unique_ptr<JavaMethodDescriptor> prepare;
    std::unique_ptr<JavaMethodDescriptor> evaluate;
    std::unique_ptr<JavaMethodDescriptor> close;
};

// Function
struct JavaUDAFContext;

class UDAFFunction {
public:
    UDAFFunction(jobject udaf_handle, FunctionContext* function_ctx, JavaUDAFContext* ctx)
            : _udaf_handle(udaf_handle), _function_context(function_ctx), _ctx(ctx) {}
    // create a new state for UDAF
    JavaGlobalRef create();
    // destroy state
    void destroy(JavaGlobalRef& state);
    // UDAF Update Function
    void update(jvalue* val);
    // UDAF merge
    void merge(jobject state, jobject buffer);
    void serialize(jobject state, jobject buffer);
    // UDAF State serialize_size
    int serialize_size(jobject state);
    // UDAF finalize
    jvalue finalize(jobject state);

    // WindowFunction reset
    void reset(jobject state);

    // WindowFunction updateBatch
    jobject window_update_batch(jobject state, int peer_group_start, int peer_group_end, int frame_start, int frame_end,
                                int col_sz, jobject* cols);

private:
    // not owned udaf function handle
    jobject _udaf_handle;
    FunctionContext* _function_context;
    JavaUDAFContext* _ctx;
};

struct JavaUDAFContext {
    JVMClass udaf_class = nullptr;
    JVMClass udaf_state_class = nullptr;
    std::unique_ptr<JavaMethodDescriptor> create;
    std::unique_ptr<JavaMethodDescriptor> destory;
    std::unique_ptr<JavaMethodDescriptor> update;
    std::unique_ptr<JavaMethodDescriptor> merge;
    std::unique_ptr<JavaMethodDescriptor> finalize;
    std::unique_ptr<JavaMethodDescriptor> serialize;
    std::unique_ptr<JavaMethodDescriptor> serialize_size;

    std::unique_ptr<JavaMethodDescriptor> reset;
    std::unique_ptr<JavaMethodDescriptor> window_update;
    std::unique_ptr<JavaMethodDescriptor> get_values;

    std::unique_ptr<DirectByteBuffer> buffer;
    // handle for UDAF object
    JavaGlobalRef handle = nullptr;
    std::vector<uint8_t> buffer_data;

    std::unique_ptr<UDAFFunction> _func;
};

// Check whether java runtime can work
Status detect_java_runtime();

} // namespace starrocks::vectorized