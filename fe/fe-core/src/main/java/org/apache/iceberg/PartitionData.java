/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.hash.Hasher;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

// copy from https://github.com/apache/iceberg/blob/apache-iceberg-1.5.0/core/src/main/java/org/apache/iceberg/PartitionData.java
public class PartitionData
        implements IndexedRecord, StructLike, SpecificData.SchemaConstructable, Serializable {

    static Schema partitionDataSchema(Types.StructType partitionType) {
        return AvroSchemaUtil.convert(partitionType, PartitionData.class.getName());
    }

    private final Types.StructType partitionType;
    private final int size;
    private final Object[] data;
    private final String stringSchema;
    private transient Schema schema;

    /** Used by Avro reflection to instantiate this class when reading manifest files. */
    PartitionData(Schema schema) {
        this.partitionType = AvroSchemaUtil.convert(schema).asNestedType().asStructType();
        this.size = partitionType.fields().size();
        this.data = new Object[size];
        this.stringSchema = schema.toString();
        this.schema = schema;
    }

    public PartitionData(Types.StructType partitionType) {
        for (Types.NestedField field : partitionType.fields()) {
            Preconditions.checkArgument(
                    field.type().isPrimitiveType(),
                    "Partitions cannot contain nested types: %s",
                    field.type());
        }

        this.partitionType = partitionType;
        this.size = partitionType.fields().size();
        this.data = new Object[size];
        this.schema = partitionDataSchema(partitionType);
        this.stringSchema = schema.toString();
    }

    /** Copy constructor */
    private PartitionData(PartitionData toCopy) {
        this.partitionType = toCopy.partitionType;
        this.size = toCopy.size;
        this.data = copyData(toCopy.partitionType, toCopy.data);
        this.stringSchema = toCopy.stringSchema;
        this.schema = toCopy.schema;
    }

    /** Copy constructor that inherits the state but replaces the partition values */
    private PartitionData(PartitionData toCopy, StructLike partition) {
        this.partitionType = toCopy.partitionType;
        this.size = toCopy.size;
        this.data = copyData(partitionType, partition);
        this.stringSchema = toCopy.stringSchema;
        this.schema = toCopy.schema;
    }

    public PartitionData(PartitionData toCopy, Object[] data) {
        this.partitionType = toCopy.partitionType;
        this.size = toCopy.size;
        this.stringSchema = toCopy.stringSchema;
        this.schema = toCopy.schema;
        this.data = data;
    }

    public Types.StructType getPartitionType() {
        return partitionType;
    }

    @Override
    public Schema getSchema() {
        if (schema == null) {
            this.schema = new Schema.Parser().parse(stringSchema);
        }
        return schema;
    }

    public Type getType(int pos) {
        return partitionType.fields().get(pos).type();
    }

    public void clear() {
        Arrays.fill(data, null);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        Object value = get(pos);
        if (value == null || javaClass.isInstance(value)) {
            return javaClass.cast(value);
        }

        throw new IllegalArgumentException(
                String.format(
                        "Wrong class, expected %s, but was %s, for object: %s",
                        javaClass.getName(), value.getClass().getName(), value));
    }

    @Override
    public Object get(int pos) {
        if (pos >= data.length) {
            return null;
        }

        if (data[pos] instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) data[pos]);
        }

        return data[pos];
    }

    @Override
    public <T> void set(int pos, T value) {
        data[pos] = toInternalValue(value);
    }

    @Override
    public void put(int i, Object v) {
        set(i, v);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PartitionData{");
        for (int i = 0; i < data.length; i += 1) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(partitionType.fields().get(i).name()).append("=").append(data[i]);
        }
        sb.append("}");
        return sb.toString();
    }

    public PartitionData copy() {
        return new PartitionData(this);
    }

    public PartitionData copyFor(StructLike partition) {
        return new PartitionData(this, partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof PartitionData)) {
            return false;
        }

        PartitionData that = (PartitionData) o;
        return partitionType.equals(that.partitionType) && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        Hasher hasher = Hashing.goodFastHash(32).newHasher();
        Stream.of(data).map(Objects::hashCode).forEach(hasher::putInt);
        partitionType.fields().stream().map(Objects::hashCode).forEach(hasher::putInt);
        return hasher.hash().hashCode();
    }

    public static Object[] copyData(Types.StructType type, Object[] data) {
        List<Types.NestedField> fields = type.fields();
        Object[] copy = new Object[data.length];
        for (int i = 0; i < data.length; i += 1) {
            if (data[i] == null) {
                copy[i] = null;
            } else {
                Types.NestedField field = fields.get(i);
                switch (field.type().typeId()) {
                    case STRUCT:
                    case LIST:
                    case MAP:
                        throw new IllegalArgumentException("Unsupported type in partition data: " + type);
                    case BINARY:
                    case FIXED:
                        byte[] buffer = (byte[]) data[i];
                        copy[i] = Arrays.copyOf(buffer, buffer.length);
                        break;
                    case STRING:
                        copy[i] = data[i].toString();
                        break;
                    default:
                        // no need to copy the object
                        copy[i] = data[i];
                }
            }
        }

        return copy;
    }

    private static Object[] copyData(Types.StructType type, StructLike partition) {
        List<Types.NestedField> fields = type.fields();
        Object[] data = new Object[fields.size()];

        for (int pos = 0; pos < fields.size(); pos++) {
            Types.NestedField field = fields.get(pos);
            Class<?> javaClass = field.type().typeId().javaClass();
            data[pos] = toInternalValue(partition.get(pos, javaClass));
        }

        return data;
    }

    private static Object toInternalValue(Object value) {
        if (value instanceof Utf8) {
            // Utf8 is not Serializable
            return value.toString();
        } else if (value instanceof ByteBuffer) {
            // ByteBuffer is not Serializable
            ByteBuffer buffer = (ByteBuffer) value;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        } else {
            return value;
        }
    }
}