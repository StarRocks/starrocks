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

package org.apache.iceberg;

import org.apache.avro.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class PartitionDataTest {

    private static Types.StructType partitionType(int idOffset) {
        return Types.StructType.of(
                Types.NestedField.optional(1000 + idOffset, "dt", Types.StringType.get()),
                Types.NestedField.optional(1001 + idOffset, "region", Types.IntegerType.get()));
    }

    @Test
    public void schemaIsCachedAcrossEqualStructTypes() {
        Types.StructType t1 = partitionType(0);
        Types.StructType t2 = partitionType(0);
        assertNotSame(t1, t2, "test setup should use distinct StructType instances");
        assertEquals(t1, t2, "test setup should use structurally-equal StructType instances");

        PartitionData a = new PartitionData(t1);
        PartitionData b = new PartitionData(t2);
        PartitionData c = new PartitionData(t1);

        assertSame(a.getSchema(), b.getSchema(),
                "Avro Schema must be shared across structurally-equal partition types");
        assertSame(a.getSchema(), c.getSchema(),
                "Avro Schema must be shared across calls with the same partition type");
    }

    @Test
    public void partitionDataSchemaIsIdempotent() {
        Types.StructType t = partitionType(50);
        Schema first = PartitionData.partitionDataSchema(t);
        Schema second = PartitionData.partitionDataSchema(t);
        assertSame(first, second,
                "partitionDataSchema must return the cached Schema for repeated calls");
    }

    @Test
    public void differentStructTypesProduceDifferentSchemas() {
        // Different field IDs make the StructType non-equal even though names/types match.
        PartitionData a = new PartitionData(partitionType(0));
        PartitionData b = new PartitionData(partitionType(100));
        assertNotSame(a.getSchema(), b.getSchema(),
                "structurally-distinct partition types must not share a Schema entry");
        assertNotEquals(a.getSchema().toString(), b.getSchema().toString());
    }

    @Test
    public void serializationRoundTripPreservesSchemaAndData() throws Exception {
        Types.StructType t = partitionType(200);
        PartitionData a = new PartitionData(t);
        a.set(0, "2026-04-27");
        a.set(1, 7);

        PartitionData restored = roundTrip(a);
        assertEquals(a, restored, "round-trip must preserve partition data values");
        assertEquals(a.getSchema().toString(), restored.getSchema().toString(),
                "round-trip must preserve the persisted Avro schema");
    }

    @Test
    public void copyConstructorSharesSchemaWithSource() {
        PartitionData a = new PartitionData(partitionType(300));
        a.set(0, "x");
        a.set(1, 1);

        PartitionData copy = a.copy();
        assertSame(a.getSchema(), copy.getSchema(),
                "copy() must share Schema instance with the source");
        assertEquals(a, copy);
    }

    @Test
    public void copyForSharesSchemaWithSource() {
        PartitionData a = new PartitionData(partitionType(400));
        PartitionData replaced = a.copyFor(a);
        assertSame(a.getSchema(), replaced.getSchema(),
                "copyFor() must share Schema instance with the source");
    }

    @Test
    public void avroReflectionConstructorRetainsSchemaInstance() {
        Schema schema = PartitionData.partitionDataSchema(partitionType(500));
        PartitionData pd = new PartitionData(schema);
        assertSame(schema, pd.getSchema(),
                "Avro reflection constructor must keep the supplied Schema instance");
        assertEquals(2, pd.size());
    }

    private static PartitionData roundTrip(PartitionData input) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
            oos.writeObject(input);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (PartitionData) ois.readObject();
        }
    }
}
