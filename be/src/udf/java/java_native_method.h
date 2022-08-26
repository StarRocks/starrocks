// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <jni.h>

#pragma once

namespace starrocks::vectorized {

// Some native implementations of JavaUDF, mainly for calling some C++ functions in Java functions
// For example, calling Column::resize in Java function
//
struct JavaNativeMethods {
    // call BinaryColumn::bytes::resize
    static jlong resizeStringData(JNIEnv* env, jclass clazz, jlong columnAddr, jint byteSize);
    // get native addrs
    // for nullable column [null_array_start, data_array_start]
    static jlongArray getAddrs(JNIEnv* env, jclass clazz, jlong columnAddr);

    static jlong memory_malloc(JNIEnv* env, jclass clazz, jlong bytes);

    static void memory_free(JNIEnv* env, jclass clazz, jlong address);
};
} // namespace starrocks::vectorized
