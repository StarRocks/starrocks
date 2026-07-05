// The focused exec primitive tests do not exercise JVM integration, but
// Runtime can pull Java helper objects into this test binary.
#include <jni.h>

extern "C" __attribute__((weak)) JNIEnv* getJNIEnv(void) {
    return nullptr;
}
