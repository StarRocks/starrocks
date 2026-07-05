// Focused tests that do not exercise JVM integration can still pull Runtime's
// Java helper objects into the final binary through static library links.
#include <jni.h>

extern "C" __attribute__((weak)) JNIEnv* getJNIEnv(void) {
    return nullptr;
}
