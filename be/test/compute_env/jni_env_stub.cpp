// The focused compute env tests do not exercise JVM integration, but Runtime
// can pull Java helper objects into this test binary through filesystem paths.
#include <jni.h>

extern "C" __attribute__((weak)) JNIEnv* getJNIEnv(void) {
    return nullptr;
}
