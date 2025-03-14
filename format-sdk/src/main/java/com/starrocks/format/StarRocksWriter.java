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

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StarRocksWriter extends DataAccessor {

    private final BufferAllocator allocator;
    private final Schema schema;

    public StarRocksWriter(long tabletId,
                           String tabletRootPath,
                           long txnId,
                           Schema schema,
                           Config config) {
        this.allocator = new RootAllocator();
        this.schema = requireNonNull(schema, "Null schema.");
        if (null == schema.getFields() || schema.getFields().isEmpty()) {
            throw new IllegalArgumentException("Empty schema fields.");
        }

        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);

        this.nativePointer = createNativeWriter(
                tabletId,
                txnId,
                arrowSchema.memoryAddress(),
                requireNonNull(tabletRootPath, "Null tablet root path."),
                requireNonNull(config, "Null config.").toMap()
        );

        Optional.ofNullable(config.getUnreleasedWarningThreshold())
                .ifPresent(this::checkUnreleasedInstances);
    }

    public void open() {
        checkAndDo(() -> nativeOpen(nativePointer));
    }

    public void write(VectorSchemaRoot root) {
        checkAndDo(() -> {
            try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
                Data.exportVectorSchemaRoot(allocator, root, null, array);
                nativeWrite(nativePointer, array.memoryAddress());
            }
        });
    }

    public void flush() {
        checkAndDo(() -> nativeFlush(nativePointer));
    }

    public void finish() {
        checkAndDo(() -> nativeFinish(nativePointer));
    }

    @Override
    public void close() throws Exception {
        checkAndDo(() -> nativeClose(nativePointer));
        this.release();
    }

    @Override
    protected void doRelease(long nativePointer) {
        nativeRelease(nativePointer);
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    /* native methods */

    public native long createNativeWriter(long tabletId,
                                          long txnId,
                                          long schema,
                                          String tableRootPath,
                                          Map<String, String> options);

    public native void nativeOpen(long nativePointer);

    public native void nativeWrite(long nativePointer, long arrowArray);

    public native void nativeFlush(long nativePointer);

    public native void nativeFinish(long nativePointer);

    public native void nativeClose(long nativePointer);

    public native void nativeRelease(long nativePointer);

}
