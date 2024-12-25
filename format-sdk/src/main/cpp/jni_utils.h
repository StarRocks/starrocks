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

#include <glog/logging.h>
#include <jni.h>

#include <string>
#include <unordered_map>
#include <vector>

#define SAFE_CALL_READER_FUNCTION(reader, body)                                   \
    if (reader != nullptr) {                                                       \
        try {                                                                      \
            body;                                                                  \
        } catch (const std::exception& ex) {                                       \
            env->ThrowNew(kNativeOptExceptionClass, ex.what());                    \
        }                                                                          \
    } else {                                                                       \
        env->ThrowNew(kNativeOptExceptionClass, "Invalid tablet reader handler!"); \
    }

#define SAFE_CALL_WRITER_FUNCTION(writer, body)                                   \
    if (writer != nullptr) {                                                       \
        try {                                                                      \
            body;                                                                  \
        } catch (const std::exception& ex) {                                       \
            env->ThrowNew(kNativeOptExceptionClass, ex.what());                    \
        }                                                                          \
    } else {                                                                       \
        env->ThrowNew(kNativeOptExceptionClass, "Invalid tablet writer handler!"); \
    }

static jclass find_class(JNIEnv* env, const std::string& class_name) {
    auto clazz = env->FindClass(class_name.c_str());
    auto g_clazz = env->NewGlobalRef(clazz);
    env->DeleteLocalRef(clazz);
    return (jclass)g_clazz;
}

static std::string jstring_to_cstring(JNIEnv* env, jstring& jvalue) {
    const char* str = env->GetStringUTFChars(jvalue, nullptr);
    std::string value = std::string(str);
    env->ReleaseStringUTFChars(jvalue, str);
    return value;
}

static std::unordered_map<std::string, std::string> jhashmap_to_cmap(JNIEnv* env, jobject hashMap) {
    std::unordered_map<std::string, std::string> result;
    jclass mapClass = env->FindClass("java/util/Map");
    if (mapClass == nullptr) {
        return result;
    }

    jmethodID entrySet = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");
    if (entrySet == nullptr) {
        return result;
    }

    jobject set = env->CallObjectMethod(hashMap, entrySet);
    if (set == nullptr) {
        return result;
    }

    // Obtain an iterator over the Set
    jclass setClass = env->FindClass("java/util/Set");
    if (setClass == nullptr) {
        return result;
    }
    jmethodID iterator = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
    if (iterator == nullptr) {
        return result;
    }
    jobject iter = env->CallObjectMethod(set, iterator);
    if (iter == nullptr) {
        return result;
    }

    // Get the Iterator method IDs
    jclass iteratorClass = env->FindClass("java/util/Iterator");
    if (iteratorClass == nullptr) {
        return result;
    }
    jmethodID hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    if (hasNext == nullptr) {
        return result;
    }
    jmethodID next = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
    if (next == nullptr) {
        return result;
    }

    // Get the Entry class method IDs
    jclass entryClass = env->FindClass("java/util/Map$Entry");
    if (entryClass == nullptr) {
        return result;
    }
    jmethodID getKey = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
    if (getKey == nullptr) {
        return result;
    }
    jmethodID getValue = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");
    if (getValue == nullptr) {
        return result;
    }

    // Iterate over the entry Set
    while (env->CallBooleanMethod(iter, hasNext)) {
        jobject entry = env->CallObjectMethod(iter, next);
        jstring key = (jstring)env->CallObjectMethod(entry, getKey);
        jstring value = (jstring)env->CallObjectMethod(entry, getValue);
        const char* keyStr = env->GetStringUTFChars(key, nullptr);
        if (!keyStr) { // Out of memory
            return result;
        }
        const char* valueStr = env->GetStringUTFChars(value, nullptr);
        if (!valueStr) { // Out of memory
            env->ReleaseStringUTFChars(key, keyStr);
            return result;
        }

        result[std::string(keyStr)] = std::string(valueStr);

        env->DeleteLocalRef(entry);
        env->ReleaseStringUTFChars(key, keyStr);
        env->DeleteLocalRef(key);
        env->ReleaseStringUTFChars(value, valueStr);
        env->DeleteLocalRef(value);
    }

    return result;
}

// The jvalue is in big-endian byte-order.
// because BigInteger.toByteArray alway return big-endian byte-order byte array
// If the first bit is 1, it is a negative number.
template <typename T>
static T BigInteger_to_native_value(JNIEnv* env, jbyteArray jvalue) {
    T value = 0;

    uint32_t jvalue_bytes = env->GetArrayLength(jvalue);
    DCHECK(sizeof(T) >= jvalue_bytes);

    int8_t* src_value = env->GetByteArrayElements(jvalue, NULL);

    if (jvalue_bytes == 0) {
        env->ReleaseByteArrayElements(jvalue, src_value, 0);
        return value;
    }

    const bool is_negative = src_value[0] < 0;
    for (int i = 0; i < jvalue_bytes; i++) {
        value = value << 8;
        value = value | (src_value[i] & 0xFF);
    }
    if (is_negative) {
        typedef typename std::make_unsigned<T>::type UT;
        for (int i = sizeof(value) - 1; i >= jvalue_bytes; --i) {
            value = value | (static_cast<UT>(0xFF) << (i * 8));
        }
    }

    env->ReleaseByteArrayElements(jvalue, src_value, 0);

    return value;
}
