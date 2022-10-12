// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.utils;

/**
 * Note: All native methods has been registered by JNI launcher in BE.
 * This class must be loaded by {@link jdk.internal.loader.ClassLoaders.AppClassLoader} only
 * and can not be loaded by any other independent class loader.
 */
public final class NativeMethodHelper {
    public static native long memoryTrackerMalloc(long bytes);

    public static native void memoryTrackerFree(long address);

    // return byteAddr
    public static native long resizeStringData(long columnAddr, int byteSize);

    // [nullAddr, dataAddr]
    public static native long[] getAddrs(long columnAddr);
}
