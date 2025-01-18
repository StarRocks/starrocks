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
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class StarRocksWriter implements AutoCloseable {

    private static final LibraryHelper LIB_HELPER = new LibraryHelper();

    /**
     * The c++ StarRocksFormatWriter pointer
     */
    private long nativeWriterPoint = 0L;

    private final AtomicBoolean released = new AtomicBoolean(false);

    private final BufferAllocator allocator;
    private final Schema schema;

    public StarRocksWriter(long tabletId,
                           String tabletRootPath,
                           long txnId,
                           Schema schema,
                           Map<String, String> config) {
        this.allocator = new RootAllocator();
        this.schema = requireNonNull(schema, "Null schema.");
        if (null == schema.getFields() || schema.getFields().isEmpty()) {
            throw new IllegalArgumentException("Empty schema fields.");
        }

        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);

        this.nativeWriterPoint = createNativeWriter(
                tabletId,
                txnId,
                arrowSchema.memoryAddress(),
                requireNonNull(tabletRootPath, "Null tablet root path."),
                requireNonNull(config, "Null config."));
    }

    public void open() {
        checkAndDo(() -> nativeOpen(nativeWriterPoint));
    }

    public long write(VectorSchemaRoot root) {
        return checkAndDo(() -> {
            try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
                Data.exportVectorSchemaRoot(allocator, root, null, array);
                return nativeWrite(nativeWriterPoint, array.memoryAddress());
            }
        });
    }

    public long flush() {
        return checkAndDo(() -> nativeFlush(nativeWriterPoint));
    }

    public long finish() {
        return checkAndDo(() -> nativeFinish(nativeWriterPoint));
    }

    @Override
    public void close() throws Exception {
        checkAndDo(() -> nativeClose(nativeWriterPoint));
    }

    public void release() {
        if (released.compareAndSet(false, true)) {
            LIB_HELPER.releaseWriter(nativeWriterPoint);
            nativeWriterPoint = 0L;
        }
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    private <T, E extends Exception> T checkAndDo(ThrowingSupplier<T, E> supplier) throws E {
        if (0 == nativeWriterPoint) {
            throw new IllegalStateException("Native writer may not be created correctly.");
        }

        if (released.get()) {
            throw new IllegalStateException("Native writer is released.");
        }

        return supplier.get();
    }

    /* native methods */

    public native long createNativeWriter(long tabletId,
                                          long txnId,
                                          long schema,
                                          String tableRootPath,
                                          Map<String, String> options);

    public native long nativeOpen(long nativeWriter);

    public native long nativeWrite(long nativeWriter, long arrowArray);

    public native long nativeFlush(long nativeWriter);

    public native long nativeFinish(long nativeWriter);

    public native long nativeClose(long nativeWriter);

}
