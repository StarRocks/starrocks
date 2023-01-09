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

namespace starrocks {

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
} // namespace starrocks
