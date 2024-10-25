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

package com.starrocks.sql.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.gson.Gson;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TRowFormat;
import org.apache.commons.lang3.ObjectUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ColumnPlus {

    public static final String BIGINT = "BIGINT";
    public static final String DATETIME = "DATETIME";
    public static final String VARBINARY = "VARBINARY";
    public static final String VARCHAR = "VARCHAR";
    public static final String JSON = "JSON";

    //TODO(by satanson): Other types such as TINYINT,SMALLINT,INT, LARGEINT and etc. should added
    public static final Map<String, Type> NAME_TO_TYPE_MAP = ImmutableMap.<String, Type>builder()
            .put(BIGINT, Type.BIGINT)
            .put(DATETIME, Type.DATETIME)
            .put(VARBINARY, Type.VARBINARY)
            .put(VARCHAR, Type.VARCHAR)
            .put(JSON, Type.JSON)
            .build();
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Map<Type, Function<Object, PrettyPrinter>> TYPE_TO_SQL_MAP =
            ImmutableMap.<Type, Function<Object, PrettyPrinter>>builder()
                    .put(Type.BIGINT, ColumnPlus::numberToString)
                    .put(Type.DATETIME, ColumnPlus::dateToSql)
                    .put(Type.VARBINARY, ColumnPlus::binaryToSql)
                    .put(Type.VARCHAR, ColumnPlus::varcharToSql)
                    .put(Type.CHAR, ColumnPlus::varcharToSql)
                    .put(Type.JSON, ColumnPlus::jsonToSql)
                    .build();
    private static final Map<Type, Function<ByteBuffer, Object>> TYPE_TO_UNPACK_MAP =
            ImmutableMap.<Type, Function<ByteBuffer, Object>>builder()
                    .put(Type.BIGINT, ByteBuffer::getLong)
                    .put(Type.DATETIME, ColumnPlus::unpackDatetime)
                    .put(Type.VARBINARY, ColumnPlus::unpackString)
                    .put(Type.VARCHAR, ColumnPlus::unpackString)
                    .put(Type.CHAR, ColumnPlus::unpackString)
                    .put(Type.JSON, ColumnPlus::unpackString)
                    .build();
    private static final Map<Type, BiFunction<DataOutput, Object, Void>> TYPE_TO_PACK_MAP =
            ImmutableMap.<Type, BiFunction<DataOutput, Object, Void>>builder()
                    .put(Type.BIGINT, ColumnPlus::packBigInt)
                    .put(Type.DATETIME, ColumnPlus::packDatetime)
                    .put(Type.VARBINARY, ColumnPlus::packString)
                    .put(Type.VARCHAR, ColumnPlus::packString)
                    .put(Type.CHAR, ColumnPlus::packString)
                    .put(Type.JSON, ColumnPlus::packJson)
                    .build();
    private final Column column;
    private final Field field;
    private final boolean isPartitionColumn;
    private final boolean isBucketColumn;
    transient Function<Object, Object> getter = null;
    transient BiFunction<Object, Object, Void> setter = null;
    transient Function<ByteBuffer, Object> unpacker = null;
    transient BiFunction<DataOutput, Object, Void> packer = null;

    private ColumnPlus(Column column, Field field, boolean isPartitionColumn, boolean isBucketColumn) {
        this.column = Objects.requireNonNull(column);
        this.field = Objects.requireNonNull(field);
        this.isPartitionColumn = isPartitionColumn;
        this.isBucketColumn = isBucketColumn;
    }

    private static PrettyPrinter numberToString(Object num) {
        Preconditions.checkArgument(num != null);
        Preconditions.checkArgument(num instanceof Number);
        return new PrettyPrinter().add(num);
    }

    private static PrettyPrinter dateToSql(Object ts) {
        Preconditions.checkArgument(ts != null);
        Preconditions.checkArgument(ts instanceof Date);
        return new PrettyPrinter().addDoubleQuoted(DATE_FORMAT.format((Date) ts));
    }

    private static PrettyPrinter binaryToSql(Object bin) {
        Preconditions.checkArgument(bin != null);
        Preconditions.checkArgument(bin instanceof String);
        return new PrettyPrinter().
                add("to_binary(").addEscapedDoubleQuoted(bin.toString()).add(", ").addDoubleQuoted("utf8").add(")");
    }

    private static PrettyPrinter varcharToSql(Object str) {
        Preconditions.checkArgument(str != null);
        return PrettyPrinter.escapedDoubleQuoted(str.toString());
    }

    private static PrettyPrinter jsonToSql(Object json) {
        Preconditions.checkArgument(json != null);
        return PrettyPrinter.escapedDoubleQuoted(new Gson().toJson(json));
    }

    private static Timestamp unpackDatetime(ByteBuffer buffer) {
        return new Timestamp(buffer.getLong());
    }

    private static String unpackString(ByteBuffer buffer) {
        int length = (int) buffer.getLong();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Void packBigInt(DataOutput dataOut, Object value) {
        Preconditions.checkArgument(value.getClass().equals(Long.class));
        try {
            dataOut.writeLong(((Long) value));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static Void packDatetime(DataOutput dataOut, Object value) {
        Preconditions.checkArgument(Date.class.isAssignableFrom(value.getClass()));
        try {
            dataOut.writeLong(((Date) value).getTime());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static Void packString(DataOutput dataOut, Object value) {
        String s = value.toString();
        try {
            dataOut.writeLong(s.length());
            dataOut.write(s.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static Void packJson(DataOutput dataOut, Object value) {
        String s = new Gson().toJson(value);
        return packString(dataOut, s);
    }

    public static ColumnPlus of(Column column, Field field, boolean isPartitionColumn, boolean isBucketColumn) {
        return new ColumnPlus(column, field, isPartitionColumn, isBucketColumn);
    }

    public static ColumnPlus fieldToColumn(Field field) {
        Preconditions.checkArgument(field.isAnnotationPresent(ColumnDescription.class));
        ColumnDescription desc = field.getAnnotation(ColumnDescription.class);
        String name = ObjectUtils.isEmpty(desc.name()) ? field.getName() : desc.name();
        Type type = Objects.requireNonNull(NAME_TO_TYPE_MAP.getOrDefault(desc.type(), null));
        type = TypePlus.of(type, desc.len(), desc.precision(), desc.scale()).getType();
        boolean isKey = desc.isKey() || desc.isPartitionColumn() || desc.isBucketColumn();
        Column column = new Column(name, type, desc.nullable(), desc.comment());
        column.setIsKey(isKey);
        column.setIsAutoIncrement(desc.autoIncrement());
        return ColumnPlus.of(column, field, desc.isPartitionColumn(), desc.isBucketColumn());
    }

    public static boolean isAcceptable(Field field) {
        return field.isAnnotationPresent(ColumnDescription.class) &&
                !field.isSynthetic() &&
                !Modifier.isStatic(field.getModifiers()) &&
                !Modifier.isTransient(field.getModifiers()) &&
                !Modifier.isVolatile(field.getModifiers());
    }

    public static Function<Object, PrettyPrinter> toTuple(List<ColumnPlus> columns) {
        return object -> {
            List<PrettyPrinter> items = columns.stream().filter(columnPlus -> !columnPlus.getColumn().isAutoIncrement())
                    .map(columnPlus -> columnPlus.toSql(object))
                    .collect(Collectors.toList());
            return new PrettyPrinter().add("(").addSuperSteps(", ", items).add(")");
        };
    }

    public static <T> TRowFormat pack(T object, List<ColumnPlus> columns) {
        TRowFormat row = new TRowFormat();
        ByteBuffer nullBits = ByteBuffer.allocate(columns.size());
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dataOutput = new LittleEndianDataOutputStream(byteOut);
        for (ColumnPlus column : columns) {
            Object value = column.valueGetter().apply(object);
            if (value == null) {
                nullBits.put((byte) 0);
            } else {
                nullBits.put((byte) 1);
                column.getPacker().apply(dataOutput, value);
            }
        }
        row.setNull_bits(nullBits.array());
        try {
            dataOutput.flush();
            dataOutput.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ByteBuffer packedData = ByteBuffer.wrap(byteOut.toByteArray()).asReadOnlyBuffer();
        row.setPacked_data(packedData);
        return row;
    }

    public static <T> T unpack(Class<T> klass, List<ColumnPlus> columns, TRowFormat row) {
        Preconditions.checkArgument(columns != null && !columns.isEmpty());
        Preconditions.checkArgument(row != null);
        Preconditions.checkArgument(
                row.isSetNull_bits() && row.null_bits.remaining() == columns.size());
        Preconditions.checkArgument(row.isSetPacked_data());
        ByteBuffer packedData = row.packed_data.order(ByteOrder.LITTLE_ENDIAN);
        try {
            T obj = klass.getConstructor().newInstance();
            for (ColumnPlus column : columns) {
                byte nullBit = row.null_bits.get();
                if (nullBit == 0) {
                    column.valueSetter().apply(obj, null);
                } else {
                    Object value = column.getUnpacker().apply(packedData);
                    column.valueSetter().apply(obj, value);
                }
            }
            return obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Column getColumn() {
        return column;
    }

    public boolean isPartitionColumn() {
        return isPartitionColumn;
    }

    public boolean isBucketColumn() {
        return isBucketColumn;
    }

    public Field getField() {
        return field;
    }

    private Method getMethod() {
        char[] chs = field.getName().toCharArray();
        chs[0] = Character.toUpperCase(chs[0]);
        String getterName = "get" + String.valueOf(chs);
        try {
            return field.getDeclaringClass().getMethod(getterName);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public Function<ByteBuffer, Object> getUnpacker() {
        if (unpacker == null) {
            unpacker = Objects.requireNonNull(TYPE_TO_UNPACK_MAP.get(TypePlus.of(column.getType()).getDecayedType()));
        }
        return unpacker;
    }

    public BiFunction<DataOutput, Object, Void> getPacker() {
        if (packer == null) {
            packer = Objects.requireNonNull(TYPE_TO_PACK_MAP.get(TypePlus.of(column.getType()).getDecayedType()));
        }
        return packer;
    }

    private Method setMethod() {
        char[] chs = field.getName().toCharArray();
        chs[0] = Character.toUpperCase(chs[0]);
        String getterName = "set" + String.valueOf(chs);
        List<Method> methods = Stream.of(field.getDeclaringClass().getMethods())
                .filter(method -> method.getName().equals(getterName) && method.getParameterTypes().length == 1)
                .collect(Collectors.toList());
        Preconditions.checkArgument(!methods.isEmpty());
        if (methods.size() == 1) {
            return methods.get(0);
        }
        Optional<Method> optMethod = methods.stream()
                .filter(method -> method.getParameterTypes()[0].equals(String.class))
                .findFirst();
        return Objects.requireNonNull(optMethod.orElse(null));
    }

    public Function<Object, Object> valueGetter() {
        if (getter == null) {
            Method method = getMethod();
            getter = o -> {
                try {
                    Object value = method.invoke(o);
                    Preconditions.checkArgument(column.isAllowNull() || value != null);
                    return value;
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            };
        }
        return getter;
    }

    public BiFunction<Object, Object, Void> valueSetter() {
        if (setter == null) {
            Method method = setMethod();
            setter = (object, value) -> {
                try {
                    Preconditions.checkArgument(column.isAllowNull() || value != null);
                    method.invoke(object, value);
                    return null;
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            };
        }
        return setter;
    }

    private PrettyPrinter toSql(Object object) {
        return Objects.requireNonNull(TYPE_TO_SQL_MAP.get(TypePlus.of(column.getType()).getDecayedType()))
                .apply(valueGetter().apply(object));
    }
}