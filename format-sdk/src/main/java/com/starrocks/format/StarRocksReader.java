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
import com.starrocks.format.jni.NativeOperateException;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class StarRocksReader implements Iterator<VectorSchemaRoot> {

    private static final LibraryHelper LIB_HELPER = new LibraryHelper();

    /**
     * The c++ StarRocksFormatReader pointer.
     */
    private long nativeReaderPoint = 0L;

    private final AtomicBoolean released = new AtomicBoolean(false);

    private final BufferAllocator allocator;
    private final Schema requiredSchema;
    private final Schema outputSchema;

    private VectorSchemaRoot data;

    public StarRocksReader(long tabletId,
                           String tabletRootPath,
                           long version,
                           Schema requiredSchema,
                           Schema outputSchema,
                           Config config) {
        this.allocator = new RootAllocator();
        this.requiredSchema = requireNonNull(requiredSchema, "Null required schema");
        if (null == requiredSchema.getFields() || requiredSchema.getFields().isEmpty()) {
            throw new IllegalArgumentException("Empty required schema fields");
        }
        this.outputSchema = requireNonNull(outputSchema, "Null output schema");

        ArrowSchema requiredArrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, this.requiredSchema, null, requiredArrowSchema);

        ArrowSchema outputArrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, this.outputSchema, null, outputArrowSchema);

        this.nativeReaderPoint = createNativeReader(
                tabletId,
                version,
                requiredArrowSchema.memoryAddress(),
                outputArrowSchema.memoryAddress(),
                tabletRootPath,
                config.toMap()
        );
    }

    public void open() {
        checkAndDo(() -> nativeOpen(nativeReaderPoint));
    }

    @Override
    public boolean hasNext() {
        data = checkAndDo(() -> {
            VectorSchemaRoot root = VectorSchemaRoot.create(outputSchema, allocator);
            try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
                try {
                    nativeGetNext(nativeReaderPoint, array.memoryAddress());
                    Data.importIntoVectorSchemaRoot(allocator, array, root, null);
                } catch (Exception e) {
                    array.release();
                    throw new NativeOperateException(e.getMessage(), e);
                }
            }
            return root;
        });
        return null != data && data.getRowCount() > 0;
    }

    @Override
    public VectorSchemaRoot next() {
        return checkAndDo(() -> data);
    }

    public void close() {
        checkAndDo(() -> nativeClose(nativeReaderPoint));
    }

    public void release() {
        if (released.compareAndSet(false, true)) {
            LIB_HELPER.releaseReader(nativeReaderPoint);
            nativeReaderPoint = 0L;
        }
    }

    private <T, E extends Exception> T checkAndDo(ThrowingSupplier<T, E> supplier) throws E {
        if (0 == nativeReaderPoint) {
            throw new IllegalStateException("Native reader may not be created correctly.");
        }

        if (released.get()) {
            throw new IllegalStateException("Native reader is released.");
        }

        return supplier.get();
    }

    /* native methods */

    public native long createNativeReader(long tabletId,
                                          long version,
                                          long requiredArrowSchemaAddr,
                                          long outputArrowSchemaAddr,
                                          String tableRootPath,
                                          Map<String, String> options);

    public native long nativeOpen(long nativeReader);

    public native long nativeClose(long nativeReader);

    public native long nativeGetNext(long nativeReader, long arrowArray);

}
