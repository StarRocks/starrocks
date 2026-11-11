// Shared across the focused test binaries under be/test. These targets do not
// exercise JVM integration, but the Runtime library they link references
// getJNIEnv() (implemented by libhdfs, see be/src/runtime/java/jni_env.h) and
// JNI_GetCreatedJavaVMs() (implemented by libjvm, see be/src/runtime/java/jvm_metrics.cpp),
// which those binaries do not link. These weak fallbacks satisfy the references; the
// real definitions (strong symbols) always win wherever they are linked.
#include <jni.h>

extern "C" __attribute__((weak)) JNIEnv* getJNIEnv(void) {
    return nullptr;
}

// No JVM exists in these binaries.
extern "C" __attribute__((weak)) jint JNI_GetCreatedJavaVMs(JavaVM** vms, jsize size, jsize* num_vms) {
    if (num_vms != nullptr) {
        *num_vms = 0;
    }
    return JNI_OK;
}
