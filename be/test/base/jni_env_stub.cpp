// Shared across the focused test binaries under be/test. These targets do not
// exercise JVM integration, but the Runtime library they link references
// getJNIEnv() (implemented by libhdfs, see be/src/runtime/java/jni_env.h), which
// those binaries do not link. This weak fallback satisfies the reference; the
// real libhdfs definition (a strong symbol) always wins wherever it is linked.
#include <jni.h>

extern "C" __attribute__((weak)) JNIEnv* getJNIEnv(void) {
    return nullptr;
}