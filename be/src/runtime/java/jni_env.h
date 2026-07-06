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

// Implemented by libhdfs:
// hadoop-hdfs-native-client/src/main/native/libhdfs/jni_helper.c
//
// StarRocks intentionally uses this function instead of attaching directly:
// 1. A thread cannot attach to more than one virtual machine.
// 2. libhdfs depends on this function and performs initialization through it.
//    If the JVM already exists, libhdfs reuses it instead of creating another.
extern "C" JNIEnv* getJNIEnv(void);
