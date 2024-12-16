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

#include <arrow/status.h>
#include <jni.h>

#include <string>

#include "format/starrocks_format_writer.h"
#include "jni_utils.h"
#include "starrocks_format/starrocks_lib.h"

namespace starrocks::lake::format {

#ifdef __cplusplus
extern "C" {
#endif

static jint jniVersion = JNI_VERSION_1_8;

jclass kNativeOptExceptionClass;

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
        return JNI_ERR;
    }

    kNativeOptExceptionClass = find_class(env, "com/starrocks/format/jni/NativeOperateException");

#ifdef DEBUG
    FLAGS_logtostderr = 1;
#endif
    // logging
    // google::InitGoogleLogging("starrocks_format");

    starrocks_format_initialize();
    return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
    JNIEnv* env;

    vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
    env->DeleteGlobalRef(kNativeOptExceptionClass);
}

/* writer functions */

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_createNativeWriter(JNIEnv* env, jobject jobj,
                                                                                     jlong jtablet_id, jlong jtxn_id,
                                                                                     jlong jschema,
                                                                                     jstring jtable_root_path,
                                                                                     jobject joptions) {
    int64_t tablet_id = jtablet_id;
    int64_t txn_id = jtxn_id;
    // get schema
    if (jschema == 0) {
        env->ThrowNew(kNativeOptExceptionClass, "output_schema should not be null");
        return 0;
    }

    std::string table_root_path = jstring_to_cstring(env, jtable_root_path);
    std::unordered_map<std::string, std::string> options = jhashmap_to_cmap(env, joptions);
    auto&& result = StarRocksFormatWriter::create(tablet_id, std::move(table_root_path), txn_id,
                                                  reinterpret_cast<struct ArrowSchema*>(jschema), std::move(options));
    if (!result.ok()) {
        env->ThrowNew(kNativeOptExceptionClass, result.status().message().c_str());
        return 0;
    }
    StarRocksFormatWriter* format_writer = std::move(result).ValueUnsafe();
    return reinterpret_cast<int64_t>(format_writer);
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeOpen(JNIEnv* env, jobject jobj, jlong handler) {
    StarRocksFormatWriter* format_writer = reinterpret_cast<StarRocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(format_writer, {
        arrow::Status st = format_writer->open();
        if (!st.ok()) {
            env->ThrowNew(kNativeOptExceptionClass, st.message().c_str());
        }
    });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeWrite(JNIEnv* env, jobject jobj, jlong handler,
                                                                              jlong jArrowArray) {
    StarRocksFormatWriter* format_writer = reinterpret_cast<StarRocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(format_writer, {
        const ArrowArray* c_array_import = reinterpret_cast<struct ArrowArray*>(jArrowArray);
        if (c_array_import != nullptr) {
            arrow::Status st = format_writer->write(c_array_import);
            if (!st.ok()) {
                env->ThrowNew(kNativeOptExceptionClass, st.message().c_str());
            }
        }
    });

    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeFlush(JNIEnv* env, jobject jobj,
                                                                              jlong handler) {
    StarRocksFormatWriter* format_writer = reinterpret_cast<StarRocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(format_writer, {
        arrow::Status st = format_writer->flush();
        if (!st.ok()) {
            env->ThrowNew(kNativeOptExceptionClass, st.message().c_str());
        }
    });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeFinish(JNIEnv* env, jobject jobj,
                                                                               jlong handler) {
    StarRocksFormatWriter* format_writer = reinterpret_cast<StarRocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(format_writer, {
        arrow::Status st = format_writer->finish();
        if (!st.ok()) {
            env->ThrowNew(kNativeOptExceptionClass, st.message().c_str());
        }
    });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeClose(JNIEnv* env, jobject jobj,
                                                                              jlong handler) {
    StarRocksFormatWriter* format_writer = reinterpret_cast<StarRocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(format_writer, { format_writer->close(); });
    return 0;
}

JNIEXPORT void JNICALL Java_com_starrocks_format_jni_LibraryHelper_releaseWriter(JNIEnv* env, jobject jobj,
                                                                                 jlong writerAddress) {
    StarRocksFormatWriter* tablet_writer = reinterpret_cast<StarRocksFormatWriter*>(writerAddress);
    if (tablet_writer != nullptr) {
        delete tablet_writer;
    }
}

#ifdef __cplusplus
}
#endif

} // namespace starrocks::lake::format