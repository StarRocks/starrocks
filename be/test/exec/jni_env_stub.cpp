// The focused exec tests do not exercise JVM integration, but Runtime can pull
// Java helper objects into these test binaries.
#include <jni.h>

extern "C" __attribute__((weak)) JNIEnv* getJNIEnv(void) {
    return nullptr;
}