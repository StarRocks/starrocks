// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.udf;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java-side entry point for vectorized ("input"="arrow") Java UDFs.
 *
 * <p>Columns are exchanged with the BE using the Apache Arrow C Data Interface
 * ({@link org.apache.arrow.c.Data}): the BE exports an {@code arrow::RecordBatch}
 * into a pair of C structs ({@code ArrowSchema}/{@code ArrowArray}) whose addresses
 * are passed here as {@code long}s. We import them zero-copy, hand one
 * {@link FieldVector} per SQL argument to the user's {@code evaluate}/{@code process}/
 * {@code update} method, and export the result back into a BE-owned pair of C structs.
 *
 * <p>Memory ownership contract:
 * <ul>
 *   <li>Input: the BE owns the input C structs and their release callback; we
 *       {@link ArrowArray#wrap wrap} them, {@code import} (which moves the release
 *       callback into the returned root), and {@code close} the root when done —
 *       that fires the BE-side release callback, freeing the exported input buffers.</li>
 *   <li>Output: the user allocates the result vector from the framework-managed
 *       allocator (obtainable via {@code arg.getAllocator()}). We
 *       {@link Data#exportVector export} it into the BE-owned output C structs, which
 *       <em>retains</em> a reference on the buffers; we then drop our own reference by
 *       closing the result vector. The BE frees the buffers when it releases the
 *       imported array. The output C-struct wrappers are NOT closed here — ownership
 *       has transferred to the BE.</li>
 *   <li>The {@link BufferAllocator} is per-stub (per driver) and outlives every
 *       in-flight batch; it is closed only at stub teardown, where its non-zero
 *       outstanding-allocation check doubles as a leak assertion.</li>
 * </ul>
 *
 * <p>All methods are invoked from the BE on a dedicated JNI pthread.
 */
public class ArrowUDFHelper {
    // long handle -> allocator, so the BE call sites stay jlong-only (mirrors NativeMethodHelper).
    private static final ConcurrentHashMap<Long, BufferAllocator> ALLOCATORS = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);

    private ArrowUDFHelper() {
    }

    /** Create a per-stub allocator and return its handle. */
    public static long createAllocator() {
        long handle = NEXT_HANDLE.getAndIncrement();
        ALLOCATORS.put(handle, new RootAllocator());
        return handle;
    }

    /** Close and drop the per-stub allocator. Throws if any buffer is still outstanding (leak). */
    public static void closeAllocator(long handle) {
        BufferAllocator allocator = ALLOCATORS.remove(handle);
        if (allocator != null) {
            allocator.close();
        }
    }

    /**
     * Scalar arrow UDF: invoke {@code evaluate(FieldVector...)} on the whole batch.
     *
     * @param udf         the user UDF instance
     * @param evaluate    the resolved {@code evaluate} method
     * @param allocHandle handle returned by {@link #createAllocator()}
     * @param inSchemaAddr address of the input {@code ArrowSchema} C struct
     * @param inArrayAddr  address of the input {@code ArrowArray} C struct
     * @param outSchemaAddr address of the (BE-owned) output {@code ArrowSchema} C struct
     * @param outArrayAddr  address of the (BE-owned) output {@code ArrowArray} C struct
     */
    public static void evaluateArrow(Object udf, Method evaluate, long allocHandle,
                                     long inSchemaAddr, long inArrayAddr,
                                     long outSchemaAddr, long outArrayAddr) throws Exception {
        BufferAllocator allocator = getAllocator(allocHandle);
        CDataDictionaryProvider provider = new CDataDictionaryProvider();
        VectorSchemaRoot root = importRoot(allocator, provider, inArrayAddr, inSchemaAddr);
        try {
            List<FieldVector> args = root.getFieldVectors();
            Object result = invokeVectorized(udf, evaluate, args, 0);
            if (!(result instanceof FieldVector)) {
                throw new RuntimeException("arrow UDF '" + evaluate.getName()
                        + "' must return an org.apache.arrow.vector.FieldVector, got "
                        + (result == null ? "null" : result.getClass().getName()));
            }
            FieldVector resultVector = (FieldVector) result;
            exportResult(allocator, provider, resultVector, outArrayAddr, outSchemaAddr);
            // Drop our producer reference now that the export holds its own — but not if the UDF
            // returned one of the input vectors unchanged (identity), which the root owns/closes.
            if (!aliasesInput(args, resultVector)) {
                resultVector.close();
            }
        } finally {
            root.close();
            provider.close();
        }
    }

    /**
     * Arrow UDTF: invoke {@code process(FieldVector...)} on the whole batch and return the boxed
     * per-input-row output arrays ({@code T[][]}). The BE drains these into the output column and
     * builds the row-expansion offsets. Input is imported/released within this call (output is
     * boxed, so no arrow buffers outlive it — a per-call allocator suffices).
     */
    public static Object[][] evaluateUDTF(Object udtf, Method process, long inSchemaAddr, long inArrayAddr)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                CDataDictionaryProvider provider = new CDataDictionaryProvider();
                VectorSchemaRoot root = importRoot(allocator, provider, inArrayAddr, inSchemaAddr)) {
            Object result = invokeVectorized(udtf, process, root.getFieldVectors(), 0);
            if (result != null && !(result instanceof Object[][])) {
                throw new RuntimeException("arrow UDTF '" + process.getName()
                        + "' must return a 2-D array (T[][]), got " + result.getClass().getName());
            }
            return (Object[][]) result;
        }
    }

    /**
     * Arrow UDAF single-state update: invoke {@code update(State, FieldVector...)} on the whole
     * batch destined for {@code state}. State/merge/serialize/finalize stay on the boxed path, so
     * only input columns are arrow and a per-call allocator suffices.
     */
    public static void batchUpdateSingle(Object udaf, Method update, Object state, long inSchemaAddr, long inArrayAddr)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                CDataDictionaryProvider provider = new CDataDictionaryProvider();
                VectorSchemaRoot root = importRoot(allocator, provider, inArrayAddr, inSchemaAddr)) {
            invokeVectorized(udaf, update, root.getFieldVectors(), 1, state);
        }
    }

    // True if the UDF returned one of its input vectors unchanged (identity); such a vector is
    // owned by the input root, so the caller must not close it a second time.
    private static boolean aliasesInput(List<FieldVector> inputs, FieldVector result) {
        for (FieldVector in : inputs) {
            if (in == result) {
                return true;
            }
        }
        return false;
    }

    static BufferAllocator getAllocator(long handle) {
        BufferAllocator allocator = ALLOCATORS.get(handle);
        if (allocator == null) {
            throw new IllegalStateException("arrow UDF allocator handle not found: " + handle);
        }
        return allocator;
    }

    /** Import a BE-exported RecordBatch (moves the release callback into the returned root). */
    static VectorSchemaRoot importRoot(BufferAllocator allocator, CDataDictionaryProvider provider,
                                       long arrayAddr, long schemaAddr) {
        try (ArrowArray inArray = ArrowArray.wrap(arrayAddr);
                ArrowSchema inSchema = ArrowSchema.wrap(schemaAddr)) {
            return Data.importVectorSchemaRoot(allocator, inArray, inSchema, provider);
        }
    }

    /**
     * Export the user's result vector into the BE-owned output C structs. exportVector retains its
     * own reference on the buffers (freed when the BE releases the imported array), so the caller
     * still owns {@code result} afterwards and is responsible for closing it.
     */
    static void exportResult(BufferAllocator allocator, CDataDictionaryProvider provider,
                             FieldVector result, long outArrayAddr, long outSchemaAddr) {
        // Wrap, but do NOT close, the output structs: the BE owns them and will invoke release.
        ArrowArray outArray = ArrowArray.wrap(outArrayAddr);
        ArrowSchema outSchema = ArrowSchema.wrap(outSchemaAddr);
        Data.exportVector(allocator, result, provider, outArray, outSchema);
    }

    /**
     * Invoke a method whose value arguments are all {@link FieldVector}s, starting at
     * {@code argOffset} of {@code prefix} (used by UDAF to prepend the state object).
     */
    static Object invokeVectorized(Object target, Method method, List<FieldVector> vectors,
                                   int prefixCount, Object... prefix) throws Exception {
        Class<?>[] paramTypes = method.getParameterTypes();
        int total = prefixCount + vectors.size();
        if (method.isVarArgs()) {
            int fixed = paramTypes.length - 1;
            Object[] callArgs = new Object[paramTypes.length];
            for (int i = 0; i < prefixCount; i++) {
                callArgs[i] = prefix[i];
            }
            // Fixed FieldVector params after the prefix.
            int vecIdx = 0;
            for (int i = prefixCount; i < fixed; i++) {
                callArgs[i] = vectors.get(vecIdx++);
            }
            // Remaining vectors packed into the varargs FieldVector[] component.
            Class<?> compType = paramTypes[paramTypes.length - 1].getComponentType();
            int restCount = vectors.size() - vecIdx;
            Object varArr = Array.newInstance(compType, restCount);
            for (int i = 0; i < restCount; i++) {
                Array.set(varArr, i, vectors.get(vecIdx++));
            }
            callArgs[paramTypes.length - 1] = varArr;
            return method.invoke(target, callArgs);
        }
        if (paramTypes.length != total) {
            throw new RuntimeException("arrow UDF '" + method.getName() + "' expects " + paramTypes.length
                    + " arguments but received " + total);
        }
        Object[] callArgs = new Object[total];
        for (int i = 0; i < prefixCount; i++) {
            callArgs[i] = prefix[i];
        }
        for (int i = 0; i < vectors.size(); i++) {
            callArgs[prefixCount + i] = vectors.get(i);
        }
        return method.invoke(target, callArgs);
    }
}
