// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.format;


import com.starrocks.format.jni.LibraryHelper;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DataAccessor {

    static {
        LibraryHelper.load();
    }

    /**
     * Native read or write pointer.
     */
    protected long nativePointer = 0L;

    protected final AtomicBoolean released = new AtomicBoolean(false);

    @FunctionalInterface
    protected interface ThrowingProcedure<E extends Exception> {

        void invoke() throws E;

    }

    @FunctionalInterface
    protected interface ThrowingSupplier<T, E extends Exception> {

        T get() throws E;

    }

    protected <E extends Exception> void checkAndDo(ThrowingProcedure<E> supplier) throws E {
        if (0 == nativePointer) {
            throw new IllegalStateException("Native reader or writer may not be created correctly.");
        }

        if (released.get()) {
            throw new IllegalStateException("Native reader or writer is released.");
        }

        supplier.invoke();
    }

    protected <T, E extends Exception> T checkAndDo(ThrowingSupplier<T, E> supplier) throws E {
        if (0 == nativePointer) {
            throw new IllegalStateException("Native reader or writer may not be created correctly.");
        }

        if (released.get()) {
            throw new IllegalStateException("Native reader or writer is released.");
        }

        return supplier.get();
    }

}
