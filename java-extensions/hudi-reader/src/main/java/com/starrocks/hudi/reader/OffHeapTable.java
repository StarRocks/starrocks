// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.hudi.reader;

import java.util.HashMap;
import java.util.Map;

/**
 * We use off-heap memory to save Hudi MOR table data
 * and a custom memory layout to be parsed by Starrocks BE written in C++.
 *
 * Off-heap table memory layout details:
 * 1. A single data column is stored continuously in off-heap memory.
 * 2. Different data columns are stored in different locations in off-heap memory.
 * 3. Introduce null indicator columns to determine if a row of the related data column is empty.
 * 4. Introduce a meta column to save the memory addresses of different data columns,
 *    the memory addresses of null indicator columns and number of rows.
 *
 * Meta column layout:
 * Meta column start address: | number of rows |
 *                            | null indicator start address of fixed length column-A |
 *                            | data column start address of the fixed length column-A  |
 *                            | ... |
 *                            | null indicator start address of variable length column-B |
 *                            | offset column start address of the variable length column-B |
 *                            | length column start address of the variable length column-B |
 *                            | data column start address of the variable length column-B |
 *                            | ... |
 *
 * Null indicator column layout:
 * Null column start address: | 1-byte boolean | 1-byte boolean | 1-byte boolean | ... |
 *                 Row index: -------row 0-------------row 1------------row 2----- ... -
 *
 * Data column layout:
 * Data columns are divided into two storage types: fixed length column and variable length.
 *
 * For fixed length column like BOOLEAN/INT/LONG, we use first-level index addressing method.
 * (1) Get data column start address from meta column.
 * (2) Use column start address to read the data of fixed length.
 * Fixed length column memory layout:
 * Data column start address of fixed length column: | X-bytes | X-bytes | X-bytes | ... |
 * INT column of 4 bytes for example:
 * Fixed length column start address: | 4-bytes INT | 4-bytes INT | 4-bytes INT | ... |
 *                         Row index:  ----row 0---------row 1---------row 2----- ... -
 *
 *
 * For variable length column like STRING/DECIMAL, we use secondary-level index addressing method.
 * (1) Get offset column start address and length column start address from meta column.
 * (2) Get the field start memory address from offset column at a row index.
 * (3) Get the field length from length column at a row index.
 * (4) Use the field start address and the field length to read the data of variable length.
 * Variable length column memory layout:
 * Offset column start address of variable length column: : | 4-bytes INT | 4-bytes INT | 4-bytes INT | ... |
 * Length column start address of variable length column: : | 4-bytes INT | 4-bytes INT | 4-bytes INT | ... |
 * Data column start address of variable length column: | X-bytes | Y-bytes | Z-bytes | ... |
 * STRING column for example:
 * Offset column start address: | 4-bytes INT | 4-bytes INT | 4-bytes INT | ... |
 *                   Row index:  ----row 0---------row 1---------row 2----- ... -
 * Length column start address: | 4-bytes INT | 4-bytes INT | 4-bytes INT | ... |
 *                   Row index:  ----row 0---------row 1---------row 2----- ... -
 * Variable length column start address: |    (length of row 0)-bytes    | (length of row 1)-bytes | ... |
 *                                       |                               |
 *                 column start address + offset of row 0    column start address + offset of row 0 + length of row 0
 */
public class OffHeapTable {

    public static Map<String, OffHeapColumnVector.OffHeapColumnType> hiveTypeMappings;
    public OffHeapColumnVector[] vectors;
    public OffHeapColumnVector.OffHeapColumnType[] types;
    public OffHeapColumnVector meta;
    public int numRows;
    public boolean[] released;

    static {
        hiveTypeMappings = new HashMap<>();
        hiveTypeMappings.put("byte", OffHeapColumnVector.OffHeapColumnType.BYTE);
        hiveTypeMappings.put("bool", OffHeapColumnVector.OffHeapColumnType.BOOLEAN);
        hiveTypeMappings.put("short", OffHeapColumnVector.OffHeapColumnType.SHORT);
        hiveTypeMappings.put("int", OffHeapColumnVector.OffHeapColumnType.INT);
        hiveTypeMappings.put("float", OffHeapColumnVector.OffHeapColumnType.FLOAT);
        hiveTypeMappings.put("bigint", OffHeapColumnVector.OffHeapColumnType.LONG);
        hiveTypeMappings.put("double", OffHeapColumnVector.OffHeapColumnType.DOUBLE);
        hiveTypeMappings.put("string", OffHeapColumnVector.OffHeapColumnType.STRING);
        hiveTypeMappings.put("date", OffHeapColumnVector.OffHeapColumnType.DATE);
    }

    public OffHeapTable(String[] storageTypes, int capacity) {
        this.types = new OffHeapColumnVector.OffHeapColumnType[storageTypes.length];
        for (int i = 0; i < storageTypes.length; i++) {
            types[i] = hiveTypeMappings.get(storageTypes[i]);
        }
        this.vectors = new OffHeapColumnVector[storageTypes.length];
        this.released = new boolean[storageTypes.length];
        int metaSize = 0;
        for (int i = 0; i < storageTypes.length; i++) {
            vectors[i] = new OffHeapColumnVector(capacity, types[i]);
            if (types[i] == OffHeapColumnVector.OffHeapColumnType.STRING
                    || types[i] == OffHeapColumnVector.OffHeapColumnType.DATE) {
                metaSize += 4;
            } else {
                metaSize += 2;
            }
            released[i] = false;
        }
        this.meta = new OffHeapColumnVector(metaSize, OffHeapColumnVector.OffHeapColumnType.LONG);
        this.numRows = 0;
    }

    public void appendData(int fieldId, Object o) {
        OffHeapColumnVector column = vectors[fieldId];
        if (o == null) {
            column.appendNull();
            return;
        }

        OffHeapColumnVector.OffHeapColumnType type = types[fieldId];
        switch (type) {
            case BOOLEAN:
                column.appendBoolean((boolean) o);
                break;
            case SHORT:
                column.appendShort((short) o);
                break;
            case INT:
                column.appendInt((int) o);
                break;
            case FLOAT:
                column.appendFloat((float) o);
                break;
            case LONG:
                column.appendLong((long) o);
                break;
            case DOUBLE:
                column.appendDouble((double) o);
                break;
            case STRING:
            case DATE:
                column.appendString(o.toString());
                break;
            default:
                throw new RuntimeException("Unhandled " + type);
        }
    }

    public void releaseOffHeapColumnVector(int fieldId) {
        if (!released[fieldId]) {
            vectors[fieldId].close();
            released[fieldId] = true;
        }
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public long getMetaNativeAddress() {
        meta.appendLong(numRows);
        for (int i = 0; i < types.length; i++) {
            OffHeapColumnVector.OffHeapColumnType type = types[i];
            OffHeapColumnVector column = vectors[i];
            if (type == OffHeapColumnVector.OffHeapColumnType.STRING || type == OffHeapColumnVector.OffHeapColumnType.DATE) {
                meta.appendLong(column.nullsNativeAddress());
                meta.appendLong(column.arrayOffsetNativeAddress());
                meta.appendLong(column.arrayLengthNativeAddress());
                meta.appendLong(column.arrayDataNativeAddress());
            } else {
                meta.appendLong(column.nullsNativeAddress());
                meta.appendLong(column.valuesNativeAddress());
            }
        }
        return meta.valuesNativeAddress();
    }

    /**
     * For test only
     */
    public void show(int limit) {
        StringBuilder sb = new StringBuilder();
        long numRows = meta.getLong(0);
        System.out.println("numRows = " + numRows);
        for (int i = 0; i < limit && i < numRows; i++) {
            for (int fieldId = 0; fieldId < types.length; fieldId++) {
                OffHeapColumnVector column = vectors[fieldId];
                if (column.isNullAt(i)) {
                    sb.append("NULL").append(", ");
                    continue;
                }
                OffHeapColumnVector.OffHeapColumnType type = types[fieldId];
                switch (type) {
                    case BOOLEAN:
                        sb.append(column.getBoolean(i)).append(", ");
                        break;
                    case SHORT:
                        sb.append(column.getShort(i)).append(", ");
                        break;
                    case INT:
                        sb.append(column.getInt(i)).append(", ");
                        break;
                    case FLOAT:
                        sb.append(column.getFloat(i)).append(", ");
                        break;
                    case LONG:
                        sb.append(column.getLong(i)).append(", ");
                        break;
                    case DOUBLE:
                        sb.append(column.getDouble(i)).append(", ");
                        break;
                    case STRING:
                    case DATE:
                        sb.append(column.getUTF8String(i)).append(", ");
                        break;
                    default:
                        throw new RuntimeException("Unhandled " + type);
                }
            }
            sb.append("\n");
        }
        System.out.println(sb);
    }

    public void close() {
        for (int i = 0; i < vectors.length; i++) {
            releaseOffHeapColumnVector(i);
        }
        meta.close();
    }
}
