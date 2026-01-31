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

package com.starrocks.common.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ArrowUtil {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    private ArrowUtil() {
    }

    public static VectorSchemaRoot createSingleSchemaRoot(String fieldName, String fieldValue) {
        VarCharVector varCharVector = new VarCharVector(fieldName, ALLOCATOR);

        varCharVector.allocateNew();
        varCharVector.setSafe(0, fieldValue.getBytes());
        varCharVector.setValueCount(1);

        return VectorSchemaRoot.of(varCharVector);
    }

    public static VarCharVector createVarCharVector(String name, BufferAllocator allocator, int valueCount) {
        VarCharVector vector = new VarCharVector(name, allocator);
        vector.allocateNew();
        vector.setValueCount(valueCount);
        return vector;
    }
}
