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

#include <stdio.h>
#include <stdlib.h>

#include "jni.h"

// about SUNWprivate_1.1 ref: https://github.com/joekoolade/JOE/blob/26f1d1cf1b7d253ee1a9752d2944cc38c52f4ced/tools/bootloader/libjvm.exp#L28
// This file is the mock file that helps us get BE started even if we don't have a JAVA_HOME configuration

_JNI_IMPORT_OR_EXPORT_ jint JNICALL JNI_GetDefaultJavaVMInitArgs(void* args) {
    return 1;
}

_JNI_IMPORT_OR_EXPORT_ jint JNICALL JNI_CreateJavaVM(JavaVM** pvm, void** penv, void* args) {
    return 1;
}

_JNI_IMPORT_OR_EXPORT_ jint JNICALL JNI_GetCreatedJavaVMs(JavaVM** vm, jsize sz, jsize* output) {
    fprintf(stderr, "unsupported call jni function, JAVA_HOME is required\n ");
    exit(1);
    return 1;
}

/* Defined by native libraries. */
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
    return 1;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved) {}
