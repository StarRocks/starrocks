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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

public class ArrowUtil {

    private ArrowUtil() {
        throw new UnsupportedOperationException("ArrowUtil cannot be instantiated");
    }

    public static VectorSchemaRoot createSingleSchemaRoot(String fieldName, String fieldValue) {
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        VarCharVector varCharVector = new VarCharVector(fieldName, allocator);
        varCharVector.allocateNew();

        varCharVector.setSafe(0, fieldValue.getBytes());
        varCharVector.setValueCount(1);

        List<Field> schemaFields = List.of(varCharVector.getField());
        List<FieldVector> dataFields = List.of(varCharVector);

        return new VectorSchemaRoot(schemaFields, dataFields);
    }
}
