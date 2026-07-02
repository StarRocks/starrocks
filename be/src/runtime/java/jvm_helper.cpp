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

#include "runtime/java/jvm_helper.h"

#include <algorithm>
#include <iterator>
#include <sstream>
#include <string>

#include "base/logging.h"
#include "base/status.h"
#include "fmt/core.h"
#include "runtime/java/java_env.h"
#include "runtime/java/jni_env.h"

#define JNI_FIND_CLASS(clazz_name)                                                                 \
    [](JNIEnv* env, const char* name) {                                                            \
        auto clazz = env->FindClass(name);                                                         \
        CHECK(clazz != nullptr) << "not found class" << name << " plz check JDK and jni-packages"; \
        auto g_clazz = env->NewGlobalRef(clazz);                                                   \
        env->DeleteLocalRef(clazz);                                                                \
        return (jclass)g_clazz;                                                                    \
    }(_env, clazz_name)

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

#define DEFINE_NEW_BOX(prim_clazz, cxx_type, CLAZZ, CallType)                                        \
    jobject JVMHelper::new##CLAZZ(cxx_type value) {                                                  \
        return getEnv()->CallStaticObjectMethod(_class_##prim_clazz, _value_of_##prim_clazz, value); \
    }                                                                                                \
    cxx_type JVMHelper::val##cxx_type(jobject obj) { return getEnv()->Call##CallType##Method(obj, _val_##prim_clazz); }

#define SET_METHOD_ID(target, clazz, name, signature)   \
    target = _env->GetMethodID(clazz, name, signature); \
    DCHECK(target != nullptr);

namespace starrocks {

JVMHelper& JVMHelper::getInstance() {
    if (_env == nullptr) {
        _env = getJNIEnv();
        CHECK(_env != nullptr) << "couldn't got a JNIEnv";
    }
    static JVMHelper helper;
    return helper;
}

std::pair<JNIEnv*, JVMHelper&> JVMHelper::getInstanceWithEnv() {
    auto& instance = getInstance();
    return {instance.getEnv(), instance};
}

void JVMHelper::_init() {
    _object_class = JNI_FIND_CLASS("java/lang/Object");
    _object_array_class = JNI_FIND_CLASS("[Ljava/lang/Object;");
    _string_class = JNI_FIND_CLASS("java/lang/String");
    _jarrays_class = JNI_FIND_CLASS("java/util/Arrays");
    _exception_util_class = JNI_FIND_CLASS("org/apache/commons/lang3/exception/ExceptionUtils");
    _big_decimal_class = JNI_FIND_CLASS("java/math/BigDecimal");
    _local_date_class = JNI_FIND_CLASS("java/time/LocalDate");
    _local_datetime_class = JNI_FIND_CLASS("java/time/LocalDateTime");

    CHECK(_object_class);
    CHECK(_object_array_class);
    CHECK(_string_class);
    CHECK(_jarrays_class);
    CHECK(_exception_util_class);
    CHECK(_big_decimal_class);
    CHECK(_local_date_class);
    CHECK(_local_datetime_class);

    _object_to_string = _env->GetMethodID(_object_class, "toString", "()Ljava/lang/String;");
    CHECK(_object_to_string);
    _object_equals = _env->GetMethodID(_object_class, "equals", "(Ljava/lang/Object;)Z");
    CHECK(_object_equals);
    _arrays_to_string = _env->GetStaticMethodID(_jarrays_class, "toString", "([Ljava/lang/Object;)Ljava/lang/String;");
    CHECK(_arrays_to_string);

    _big_decimal_ctor_string = _env->GetMethodID(_big_decimal_class, "<init>", "(Ljava/lang/String;)V");
    CHECK(_big_decimal_ctor_string);
    _big_decimal_value_of_ll = _env->GetStaticMethodID(_big_decimal_class, "valueOf", "(JI)Ljava/math/BigDecimal;");
    CHECK(_big_decimal_value_of_ll);

    _local_date_of_epoch_day = _env->GetStaticMethodID(_local_date_class, "ofEpochDay", "(J)Ljava/time/LocalDate;");
    CHECK(_local_date_of_epoch_day);
    _local_date_to_epoch_day = _env->GetMethodID(_local_date_class, "toEpochDay", "()J");
    CHECK(_local_date_to_epoch_day);

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

    _direct_buffer_class = JNI_FIND_CLASS("java/nio/ByteBuffer");
    SET_METHOD_ID(_direct_buffer_clear, _direct_buffer_class, "clear", "()Ljava/nio/Buffer;");

    auto list_clazz = JNI_FIND_CLASS("java/util/List");
    DCHECK(list_clazz);
    _list_meta.list_class = new JVMClass(list_clazz);
    auto array_list_clazz = JNI_FIND_CLASS("java/util/ArrayList");
    DCHECK(array_list_clazz);
    _list_meta.array_list_class = new JVMClass(array_list_clazz);

    SET_METHOD_ID(_list_meta.list_get, list_clazz, "get", "(I)Ljava/lang/Object;");
    SET_METHOD_ID(_list_meta.list_size, list_clazz, "size", "()I");
    SET_METHOD_ID(_list_meta.list_add, list_clazz, "add", "(Ljava/lang/Object;)Z");

    auto map_clazz = JNI_FIND_CLASS("java/util/Map");
    DCHECK(map_clazz);
    _map_class = new JVMClass(map_clazz);
}

#define CHECK_FUNCTION_EXCEPTION(_env, name)                   \
    if (auto e = _env->ExceptionOccurred()) {                  \
        LOCAL_REF_GUARD(e);                                    \
        _env->ExceptionClear();                                \
        LOG(WARNING) << "Exception happened when call " #name; \
        return "";                                             \
    }

#define RETURN_ERROR_IF_EXCEPTION(env, errmsg)                            \
    if (jthrowable jthr = env->ExceptionOccurred()) {                     \
        LOCAL_REF_GUARD(jthr);                                            \
        std::string msg = fmt::format(errmsg, dumpExceptionString(jthr)); \
        LOG(WARNING) << msg;                                              \
        env->ExceptionClear();                                            \
        return Status::RuntimeError(msg);                                 \
    }

Status JVMHelper::_check_exception_status() {
    RETURN_ERROR_IF_EXCEPTION(_env, "exception happened when invoke method: {}");
    return Status::OK();
}

std::string JVMHelper::array_to_string(jobject object) {
    auto* env = getEnv();
    env->ExceptionClear();
    jobject jstr = env->CallStaticObjectMethod(_jarrays_class, _arrays_to_string, object);
    LOCAL_REF_GUARD(jstr);
    CHECK_FUNCTION_EXCEPTION(env, "array_to_string")
    return to_cxx_string((jstring)jstr);
}

bool JVMHelper::equals(jobject obj1, jobject obj2) {
    auto* env = getEnv();
    env->ExceptionClear();
    auto res = env->CallBooleanMethod(obj1, _object_equals, obj2);
    if (auto e = env->ExceptionOccurred()) {
        LOCAL_REF_GUARD(e);
        env->ExceptionClear();
        LOG(WARNING) << "Exception happened when call equals";
        return false;
    }
    return res;
}

std::string JVMHelper::to_string(jobject obj) {
    auto* env = getEnv();
    env->ExceptionClear();
    auto res = env->CallObjectMethod(obj, _object_to_string);
    LOCAL_REF_GUARD(res);
    CHECK_FUNCTION_EXCEPTION(env, "to_string")
    return to_cxx_string((jstring)res);
}

std::string JVMHelper::to_cxx_string(jstring str) {
    if (str == nullptr) {
        return "<null>";
    }
    auto* env = getEnv();
    std::string res;
    const char* charflow = env->GetStringUTFChars(str, nullptr);
    res = charflow;
    env->ReleaseStringUTFChars(str, charflow);
    return res;
}

std::string JVMHelper::dumpExceptionString(jthrowable throwable) {
    auto* env = getEnv();
    auto get_stack_trace =
            env->GetStaticMethodID(_exception_util_class, "getStackTrace", "(Ljava/lang/Throwable;)Ljava/lang/String;");
    CHECK(get_stack_trace != nullptr) << "Not Found JNI method getStackTrace";
    jobject stack_traces = env->CallStaticObjectMethod(_exception_util_class, get_stack_trace, (jobject)throwable);
    LOCAL_REF_GUARD(stack_traces);
    // don't call return if xxx, to avoid recursive
    env->ExceptionClear();
    return to_cxx_string((jstring)stack_traces);
}

jmethodID JVMHelper::getToStringMethod(jclass clazz) {
    return getEnv()->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
}

StatusOr<jstring> JVMHelper::to_jstring(const std::string& str) {
    auto* env = getEnv();
    auto res = env->NewStringUTF(str.c_str());
    if (UNLIKELY(res == nullptr)) {
        env->ExceptionClear();
        return Status::InternalError(fmt::format("NewStringUTF failed for: {}", str));
    }
    return res;
}

jmethodID JVMHelper::getMethod(jclass clazz, const std::string& method, const std::string& sig) {
    return getEnv()->GetMethodID(clazz, method.c_str(), sig.c_str());
}

jmethodID JVMHelper::getStaticMethod(jclass clazz, const std::string& method, const std::string& sig) {
    return getEnv()->GetStaticMethodID(clazz, method.c_str(), sig.c_str());
}

jobject JVMHelper::create_array(int sz) {
    auto* env = getEnv();
    auto res = env->NewObjectArray(sz, _object_class, nullptr);
    RETURN_IF_JNI_EXCEPTION(env, "create_array: NewObjectArray failed", nullptr);
    return res;
}

jobject JVMHelper::create_object_array(jobject o, int num_rows) {
    auto* env = getEnv();
    jobjectArray res_arr = env->NewObjectArray(num_rows, _object_array_class, o);
    RETURN_IF_JNI_EXCEPTION(env, "create_object_array: NewObjectArray failed", nullptr);
    return res_arr;
}

jobjectArray JVMHelper::build_object_array(jclass clazz, jobject* arr, int sz) {
    auto* env = getEnv();
    jobjectArray res_arr = env->NewObjectArray(sz, clazz, nullptr);
    RETURN_IF_JNI_EXCEPTION(env, "build_object_array: NewObjectArray failed", nullptr);
    for (int i = 0; i < sz; ++i) {
        env->SetObjectArrayElement(res_arr, i, arr[i]);
    }
    return res_arr;
}

DEFINE_NEW_BOX(boolean, uint8_t, Boolean, Boolean);
DEFINE_NEW_BOX(byte, int8_t, Byte, Byte);
DEFINE_NEW_BOX(short, int16_t, Short, Short);
DEFINE_NEW_BOX(int, int32_t, Integer, Int);
DEFINE_NEW_BOX(long, int64_t, Long, Long);
DEFINE_NEW_BOX(float, float, Float, Float);
DEFINE_NEW_BOX(double, double, Double, Double);

jobject JVMHelper::newString(const char* data, size_t size) {
    auto* env = getEnv();
    auto bytesArr = env->NewByteArray(size);
    RETURN_IF_JNI_EXCEPTION(env, "newString: NewByteArray failed", nullptr);
    LOCAL_REF_GUARD(bytesArr);
    env->SetByteArrayRegion(bytesArr, 0, size, reinterpret_cast<const jbyte*>(data));
    jobject nstr = env->NewObject(_string_class, _string_construct_with_bytes, bytesArr, _utf8_charsets);
    RETURN_IF_JNI_EXCEPTION(env, "newString: NewObject failed", nullptr);
    return nstr;
}

jobject JVMHelper::newBigDecimal(const std::string& s) {
    auto* env = getEnv();
    jobject jstr = newString(s.data(), s.size());
    if (jstr == nullptr) {
        return nullptr;
    }
    LOCAL_REF_GUARD(jstr);
    jobject bd = env->NewObject(_big_decimal_class, _big_decimal_ctor_string, jstr);
    RETURN_IF_JNI_EXCEPTION(env, "newBigDecimal: NewObject failed", nullptr);
    return bd;
}

jobject JVMHelper::newBigDecimal(int64_t unscaled, int scale) {
    auto* env = getEnv();
    jobject bd = env->CallStaticObjectMethod(_big_decimal_class, _big_decimal_value_of_ll, static_cast<jlong>(unscaled),
                                             static_cast<jint>(scale));
    RETURN_IF_JNI_EXCEPTION(env, "newBigDecimal(long, int): CallStatic failed", nullptr);
    return bd;
}

Slice JVMHelper::sliceVal(jstring jstr, std::string* buffer) {
    auto* env = getEnv();
    const size_t utf_length = env->GetStringUTFLength(jstr);
    buffer->resize(utf_length);
    const size_t string_length = env->GetStringLength(jstr);
    env->GetStringUTFRegion(jstr, 0, string_length, buffer->data());
    return {buffer->data(), buffer->length()};
}

jobject JVMHelper::newLocalDateFromEpochDay(int64_t epoch_day) {
    auto* env = getEnv();
    jobject ld =
            env->CallStaticObjectMethod(_local_date_class, _local_date_of_epoch_day, static_cast<jlong>(epoch_day));
    RETURN_IF_JNI_EXCEPTION(env, "newLocalDateFromEpochDay: ofEpochDay failed", nullptr);
    return ld;
}

int64_t JVMHelper::valLocalDateToEpochDay(jobject obj) {
    return getEnv()->CallLongMethod(obj, _local_date_to_epoch_day);
}

std::string JVMHelper::to_jni_class_name(const std::string& name) {
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

DirectByteBuffer::DirectByteBuffer(void* ptr, int capacity) {
    auto* env = JVMHelper::getInstance().getEnv();
    auto lref = env->NewDirectByteBuffer(ptr, capacity);
    LOCAL_REF_GUARD(lref);
    _handle = env->NewGlobalRef(lref);
    _capacity = capacity;
    _data = ptr;
}

DirectByteBuffer::~DirectByteBuffer() {
    if (_handle != nullptr) {
        auto st = JavaEnv::GetInstance()->call_function_in_pthread([this]() {
            JNIEnv* env = JVMHelper::getInstance().getEnv();
            env->DeleteGlobalRef(_handle);
            _handle = nullptr;
            return Status::OK();
        });
        LOG_IF(WARNING, !st.ok()) << "failed to delete direct byte buffer global ref: " << st;
    }
}

Status JavaListStub::add(jobject element) {
    auto [env, helper] = JVMHelper::getInstanceWithEnv();
    const auto& meta = helper.list_meta();
    env->CallVoidMethod(_list, meta.list_add, element);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return Status::OK();
}

StatusOr<jobject> JavaListStub::get(int index) {
    auto [env, helper] = JVMHelper::getInstanceWithEnv();
    const auto& meta = helper.list_meta();
    auto res = env->CallObjectMethod(_list, meta.list_get, index);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

StatusOr<size_t> JavaListStub::size() {
    auto [env, helper] = JVMHelper::getInstanceWithEnv();
    const auto& meta = helper.list_meta();
    auto res = env->CallIntMethod(_list, meta.list_size);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

} // namespace starrocks
