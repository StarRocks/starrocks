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

#include "udf/java/java_udf_reflection.h"

#include <string>

#include "base/utility/defer_op.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "runtime/java/java_env.h"
#include "udf/java/java_udf.h"

namespace starrocks {

namespace {

constexpr const char* CLASS_LOADER_NAME = "com.starrocks.udf.UDFClassLoader";
constexpr const char* CLASS_ANALYZER_NAME = "com.starrocks.udf.UDFClassAnalyzer";

#define RETURN_ERROR_IF_EXCEPTION(env, errmsg)                              \
    if (auto e = env->ExceptionOccurred()) {                                \
        LOCAL_REF_GUARD(e);                                                 \
        auto msg = JVMFunctionHelper::getInstance().dumpExceptionString(e); \
        env->ExceptionClear();                                              \
        return Status::InternalError(fmt::format(errmsg, msg));             \
    }

} // namespace

JavaUdfClassLoader::~JavaUdfClassLoader() {
    _handle.clear();
    _clazz.clear();
}

Status JavaUdfClassLoader::init() {
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();
    if (env == nullptr) {
        return Status::InternalError("Init JNIEnv fail");
    }
    std::string name = JVMFunctionHelper::to_jni_class_name(CLASS_LOADER_NAME);
    jclass clazz = env->FindClass(name.c_str());
    LOCAL_REF_GUARD(clazz);

    if (clazz == nullptr) {
        RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "ClassLoader Not Found");
        return Status::InternalError(fmt::format("ClassLoader Not Found: {}", CLASS_LOADER_NAME));
    }
    _clazz = env->NewGlobalRef(clazz);

    jmethodID udf_loader_contructor = env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V");
    if (udf_loader_contructor == nullptr) {
        RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "ClassLoader constructor Not Found");
        return Status::InternalError("ClassLoader constructor Not Found");
    }

    // create class loader instance
    jstring jstr = env->NewStringUTF(_path.c_str());
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "couldn't create jstring for classloader path");
    LOCAL_REF_GUARD(jstr);

    auto handle = env->NewObject(clazz, udf_loader_contructor, jstr);
    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "couldn't create classloader");
    LOCAL_REF_GUARD(handle);
    _handle = env->NewGlobalRef(handle);

    // init method id
    _get_class = env->GetMethodID((jclass)_clazz.handle(), "findClass", "(Ljava/lang/String;)Ljava/lang/Class;");
    if (_get_class == nullptr) {
        RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "couldn't get findClass method for classloader");
        return Status::InternalError("couldn't get findClass method for classloader");
    }

    _get_call_stub =
            env->GetMethodID((jclass)_clazz.handle(), "generateCallStubV",
                             "(Ljava/lang/String;Ljava/lang/Class;Ljava/lang/reflect/Method;II)Ljava/lang/Class;");
    if (_get_call_stub == nullptr) {
        RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "couldn't get generateCallStubV method for classloader");
        return Status::InternalError("couldn't get generateCallStubV method for classloader");
    }

    return Status::OK();
}

StatusOr<JVMClass> JavaUdfClassLoader::getClass(const std::string& className) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();
    CHECK(env != nullptr) << "couldn't got a JNIEnv";

    // class Name java.lang.Object -> java/lang/Object
    std::string jni_class_name = JVMFunctionHelper::to_jni_class_name(className);
    // invoke class loader
    ASSIGN_OR_RETURN(jstring jstr_name, helper.to_jstring(jni_class_name));
    LOCAL_REF_GUARD(jstr_name);

    auto loaded_clazz = env->CallObjectMethod(_handle.handle(), _get_class, jstr_name);
    LOCAL_REF_GUARD(loaded_clazz);

    // check exception
    RETURN_ERROR_IF_EXCEPTION(env, "exception happened when get class: {}");
    // no exception happened, class exists
    DCHECK(loaded_clazz != nullptr);

    auto res = env->NewGlobalRef(loaded_clazz);
    return res;
}

StatusOr<JVMClass> JavaUdfClassLoader::genCallStub(const std::string& stubClassName, jclass clazz, jobject method,
                                                   int type, int numActualVarArgs) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    std::string jni_class_name = JVMFunctionHelper::to_jni_class_name(stubClassName);
    ASSIGN_OR_RETURN(jstring jstr_name, helper.to_jstring(jni_class_name));
    LOCAL_REF_GUARD(jstr_name);

    // generate call stub; pass numActualVarArgs for varargs methods
    auto loaded_clazz =
            env->CallObjectMethod(_handle.handle(), _get_call_stub, jstr_name, clazz, method, type, numActualVarArgs);
    LOCAL_REF_GUARD(loaded_clazz);
    RETURN_ERROR_IF_EXCEPTION(env, "exception happened when gen call stub: {}");

    auto res = env->NewGlobalRef(loaded_clazz);
    return res;
}

jmethodID JavaUdfMethodDescriptor::get_method_id() const {
    return JVMFunctionHelper::getInstance().getEnv()->FromReflectedMethod(method.handle());
}

Status JavaUdfClassAnalyzer::has_method(jclass clazz, const std::string& method, bool* has) {
    DCHECK(clazz != nullptr);
    DCHECK(has != nullptr);

    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = getJNIEnv();

    std::string anlyzer_clazz_name = JVMFunctionHelper::to_jni_class_name(CLASS_ANALYZER_NAME);
    jclass class_analyzer = env->FindClass(anlyzer_clazz_name.c_str());
    LOCAL_REF_GUARD(class_analyzer);

    if (class_analyzer == nullptr) {
        return Status::InternalError(fmt::format("ClassAnalyzer Not Found: {}", CLASS_ANALYZER_NAME));
    }

    jmethodID hasMethod =
            env->GetStaticMethodID(class_analyzer, "hasMemberMethod", "(Ljava/lang/String;Ljava/lang/Class;)Z");
    if (hasMethod == nullptr) {
        return Status::InternalError("couldn't found hasMethod method");
    }

    ASSIGN_OR_RETURN(jstring method_name, helper.to_jstring(method.c_str()));
    LOCAL_REF_GUARD(method_name);

    *has = env->CallStaticBooleanMethod(class_analyzer, hasMethod, method_name, (jobject)clazz);

    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "call hasMemberMethod failed");

    return Status::OK();
}

void JavaUdfClassAnalyzer::strip_jni_generic_types(std::string* sign) {
    std::string cleaned;
    cleaned.reserve(sign->size());
    int depth = 0;
    for (char c : *sign) {
        if (c == '<') {
            depth++;
        } else if (c == '>') {
            depth--;
        } else if (depth == 0) {
            cleaned += c;
        }
    }
    *sign = std::move(cleaned);
}

Status JavaUdfClassAnalyzer::get_signature(jclass clazz, const std::string& method, std::string* sign) {
    DCHECK(clazz != nullptr);
    DCHECK(sign != nullptr);
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();
    std::string anlyzer_clazz_name = JVMFunctionHelper::to_jni_class_name(CLASS_ANALYZER_NAME);
    jclass class_analyzer = env->FindClass(anlyzer_clazz_name.c_str());
    LOCAL_REF_GUARD(class_analyzer);

    if (class_analyzer == nullptr) {
        return Status::InternalError(fmt::format("ClassAnalyzer Not Found: {}", CLASS_ANALYZER_NAME));
    }
    jmethodID getSign = env->GetStaticMethodID(class_analyzer, "getSignature",
                                               "(Ljava/lang/String;Ljava/lang/Class;)Ljava/lang/String;");
    if (getSign == nullptr) {
        return Status::InternalError("couldn't found getSignature method");
    }

    ASSIGN_OR_RETURN(jstring method_name, helper.to_jstring(method.c_str()));
    LOCAL_REF_GUARD(method_name);

    jobject result_sign = env->CallStaticObjectMethod(class_analyzer, getSign, method_name, (jobject)clazz);
    LOCAL_REF_GUARD(result_sign);

    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "call getSignature failed");

    if (result_sign == nullptr) {
        return Status::InternalError(fmt::format("couldn't found method:{}", method));
    }
    *sign = helper.to_string(result_sign);
    // Strip generic type parameters from signature to produce standard JNI method descriptor.
    // Java's getGenericParameterTypes() may produce signatures with generic info that:
    // 1. JNI GetMethodID cannot match against the erased method descriptor, returning NULL.
    // 2. get_udaf_method_desc() uses exact string matching (e.g. type == "java/util/List")
    //    to populate method_desc. Generic signatures like "java/util/List<java/lang/String>"
    //    would fail to match, causing missing JavaUdfMethodTypeDescriptor entries and subsequent
    //    out-of-bounds access in process()/update()/merge() when indexing method_desc[j+1].
    strip_jni_generic_types(sign);
    return Status::OK();
}

StatusOr<jobject> JavaUdfClassAnalyzer::get_method_object(jclass clazz, const std::string& method) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    std::string anlyzer_clazz_name = JVMFunctionHelper::to_jni_class_name(CLASS_ANALYZER_NAME);
    jclass class_analyzer = env->FindClass(anlyzer_clazz_name.c_str());
    LOCAL_REF_GUARD(class_analyzer);
    DCHECK(class_analyzer);

    jmethodID getMethodObject = env->GetStaticMethodID(
            class_analyzer, "getMethodObject", "(Ljava/lang/String;Ljava/lang/Class;)Ljava/lang/reflect/Method;");
    DCHECK(getMethodObject);
    ASSIGN_OR_RETURN(jstring method_name, helper.to_jstring(method.c_str()));
    LOCAL_REF_GUARD(method_name);

    jobject method_object = env->CallStaticObjectMethod(class_analyzer, getMethodObject, method_name, (jobject)clazz);
    LOCAL_REF_GUARD(method_object);

    RETURN_ERROR_IF_JNI_EXCEPTION_WITH_PREFIX(env, "call getMethodObject failed");

    if (method_object == nullptr) {
        return Status::InternalError(fmt::format("couldn't found method:{}", method));
    }
    auto res = env->NewGlobalRef(method_object);
    return res;
}

Status JavaUdfClassAnalyzer::get_method_desc(const std::string& sign, std::vector<JavaUdfMethodTypeDescriptor>* desc) {
    RETURN_IF_ERROR(get_udaf_method_desc(sign, desc));
    // return type may be a void type
    for (int i = 1; i < desc->size(); ++i) {
        if (desc->at(i).type == TYPE_UNKNOWN) {
            return Status::InternalError(fmt::format("unknown type sign:{}", sign));
        }
    }
    return Status::OK();
}

// clang-format off
#define ADD_BOXED_METHOD_TYPE_DESC(STR, TYPE)        \
    } else if (type == STR) {                        \
      desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE, true});

#define ADD_PRIM_METHOD_TYPE_DESC(STR, TYPE)         \
    } else if (sign[i] == STR) {                     \
      desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE, false});
// clang-format on

Status JavaUdfClassAnalyzer::get_udaf_method_desc(const std::string& sign,
                                                  std::vector<JavaUdfMethodTypeDescriptor>* desc) {
    for (int i = 0; i < sign.size(); ++i) {
        if (sign[i] == '(' || sign[i] == ')') {
            continue;
        }
        // Handle array types
        if (sign[i] == '[') {
            // Consume all leading '[' (for multi-dimensional arrays)
            while (i < sign.size() && sign[i] == '[') {
                ++i;
            }

            if (i >= sign.size()) {
                return Status::InternalError(fmt::format("Invalid array descriptor '{}': missing element type", sign));
            }

            // Handle element type: object array 'L...;' or primitive type (single char)
            // For varargs parameters (e.g. String..., Integer...), the method signature contains
            // the array type ([Ljava/lang/String; etc.). Map the element type to the corresponding
            // LogicalType so that get_method_desc() validation passes.
            LogicalType elem_type = TYPE_UNKNOWN;
            bool elem_is_box = true;
            if (sign[i] == 'L') {
                // Object array: [L<classname>;
                ++i; // Skip 'L'
                int elem_st = i;
                while (i < sign.size() && sign[i] != ';') {
                    ++i;
                }
                if (i >= sign.size()) {
                    return Status::InternalError(
                            fmt::format("Invalid object array descriptor '{}': missing ';'", sign));
                }
                std::string elem_class = sign.substr(elem_st, i - elem_st);
                if (elem_class == "java/lang/String") {
                    elem_type = TYPE_VARCHAR;
                } else if (elem_class == "java/lang/Boolean") {
                    elem_type = TYPE_BOOLEAN;
                } else if (elem_class == "java/lang/Byte") {
                    elem_type = TYPE_TINYINT;
                } else if (elem_class == "java/lang/Short") {
                    elem_type = TYPE_SMALLINT;
                } else if (elem_class == "java/lang/Integer") {
                    elem_type = TYPE_INT;
                } else if (elem_class == "java/lang/Long") {
                    elem_type = TYPE_BIGINT;
                } else if (elem_class == "java/lang/Float") {
                    elem_type = TYPE_FLOAT;
                } else if (elem_class == "java/lang/Double") {
                    elem_type = TYPE_DOUBLE;
                } else if (elem_class == "java/util/List") {
                    elem_type = TYPE_ARRAY;
                } else if (elem_class == "java/util/Map") {
                    elem_type = TYPE_MAP;
                }
                // i now points to ';', loop will increment it
            } else {
                // Primitive array: [Z [B [S [I [J [F [D
                elem_is_box = false;
                switch (sign[i]) {
                case 'Z':
                    elem_type = TYPE_BOOLEAN;
                    break;
                case 'B':
                    elem_type = TYPE_TINYINT;
                    break;
                case 'S':
                    elem_type = TYPE_SMALLINT;
                    break;
                case 'I':
                    elem_type = TYPE_INT;
                    break;
                case 'J':
                    elem_type = TYPE_BIGINT;
                    break;
                case 'F':
                    elem_type = TYPE_FLOAT;
                    break;
                case 'D':
                    elem_type = TYPE_DOUBLE;
                    break;
                default:
                    break;
                }
                elem_type = TYPE_UNKNOWN;
            }

            desc->emplace_back(JavaUdfMethodTypeDescriptor{elem_type, elem_is_box, true});
            continue;
        }
        if (sign[i] == 'L') {
            int st = i + 1;
            while (i < sign.size() && sign[i] != ';') {
                i++;
            }
            if (i >= sign.size()) {
                return Status::InternalError(fmt::format("Invalid object type descriptor '{}': missing ';'", sign));
            }
            std::string type = sign.substr(st, i - st);
            if (false) {
                // clang-format off
            ADD_BOXED_METHOD_TYPE_DESC("java/lang/Boolean", TYPE_BOOLEAN)
            ADD_BOXED_METHOD_TYPE_DESC("java/lang/Byte", TYPE_TINYINT)
            ADD_BOXED_METHOD_TYPE_DESC("java/lang/Short", TYPE_SMALLINT)
            ADD_BOXED_METHOD_TYPE_DESC("java/lang/Integer", TYPE_INT)
            ADD_BOXED_METHOD_TYPE_DESC("java/lang/Long", TYPE_BIGINT)
            ADD_BOXED_METHOD_TYPE_DESC("java/lang/Float", TYPE_FLOAT)
            ADD_BOXED_METHOD_TYPE_DESC("java/lang/Double", TYPE_DOUBLE)
                // clang-format on
            } else if (type == "java/lang/String") {
                desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_VARCHAR, true});
            } else if (type == "java/util/List") {
                desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_ARRAY, true});
            } else if (type == "java/util/Map") {
                desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_MAP, true});
            } else if (type == "java/math/BigDecimal") {
                // Structural placeholder only: `.type` is not dispatched on at runtime for
                // BigDecimal params. The actual DECIMAL precision/scale is resolved from
                // ctx->get_arg_type() / ctx->get_return_type() at the call site.
                desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_DECIMAL128, true});
            } else if (type == "java/time/LocalDate") {
                desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_DATE, true});
            } else if (type == "java/time/LocalDateTime") {
                desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_DATETIME, true});
            } else {
                // Any other object class is a user-declared record bound to a STRUCT
                // parameter/return. The FE analyzer has already validated at
                // CREATE FUNCTION time that the class is a record matching the SQL
                // STRUCT type, and that no unrecognized class appears in a non-STRUCT
                // slot, so this branch is safe to treat as TYPE_STRUCT (no further
                // class-name interpretation is needed here).
                desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_STRUCT, true});
            }
            continue;
        }
        if (false) {
            // clang-format off
        ADD_PRIM_METHOD_TYPE_DESC('Z', TYPE_BOOLEAN)
        ADD_PRIM_METHOD_TYPE_DESC('B', TYPE_TINYINT)
        ADD_PRIM_METHOD_TYPE_DESC('S', TYPE_SMALLINT)
        ADD_PRIM_METHOD_TYPE_DESC('I', TYPE_INT)
        ADD_PRIM_METHOD_TYPE_DESC('J', TYPE_BIGINT)
        ADD_PRIM_METHOD_TYPE_DESC('F', TYPE_FLOAT)
        ADD_PRIM_METHOD_TYPE_DESC('D', TYPE_DOUBLE)
            // clang-format on
        } else if (sign[i] == 'V') {
            desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_UNKNOWN, false});
        } else {
            desc->emplace_back(JavaUdfMethodTypeDescriptor{TYPE_UNKNOWN, false});
        }
    }

    if (desc->size() > 1) {
        desc->insert(desc->begin(), (*desc)[desc->size() - 1]);
        desc->erase(desc->begin() + desc->size() - 1);
    }
    return Status::OK();
}

#undef ADD_PRIM_METHOD_TYPE_DESC
#undef ADD_BOXED_METHOD_TYPE_DESC
#undef RETURN_ERROR_IF_EXCEPTION

} // namespace starrocks
