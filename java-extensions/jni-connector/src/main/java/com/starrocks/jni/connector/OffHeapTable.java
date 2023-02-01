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

package com.starrocks.jni.connector;

// @formatter:off
/**
 * We use off-heap memory to save the off-heap table data
 * and a custom memory layout to be parsed by Starrocks BE written in C++.
 *
 * Off-heap table memory layout details:
 * 1. A single data column is stored continuously in off-heap memory.
 * 2. Different data columns are stored in different locations in off-heap memory.
 * 3. Introduce null indicator columns to determine if a field is empty or not.
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
 *                            | data column start address of the variable length column-B |
 *                            | ... |
 *
 * Null indicator column layout:
 * Null column start address: | 1-byte boolean | 1-byte boolean | 1-byte boolean | ... |
 *                 Row index: -------row 0-------------row 1------------row 2----- ... -
 *
 * Data column layout:
 * Data columns are divided into two storage types: fixed length column and variable length column.
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
 * (1) Get data column start address from meta column.
 * (2) Get the field start memory address from offset column at a row index.
 * (2) Get the field start memory address from offset column at the next row index to compute the filed length.
 * (4) Use the data start address and the field length to read the data of variable length.
 * Variable length column memory layout:
 * Offset column start address of variable length column: : | 4-bytes INT | 4-bytes INT | 4-bytes INT | ... |
 * Data column start address of variable length column: | X-bytes | Y-bytes | Z-bytes | ... |
 * STRING column for example:
 * Offset column start address: | 4-bytes INT | 4-bytes INT | 4-bytes INT | ... |
 *                   Row index:  ----row 0---------row 1---------row 2----- ... -
 * Variable length column start address: |    (length of row 0)-bytes    | (length of row 1)-bytes | ... |
 *                                       |                               |
 *                 column start address + offset of row 0    column start address + offset of row 1
 */
// @formatter:on

public class OffHeapTable {
    public OffHeapColumnVector[] vectors;
    public String[] fields;
    public OffHeapColumnVector meta;
    public int numRows;
    public boolean[] released;

    public OffHeapTable(ColumnType[] types, String[] fields, int capacity) {
        this.fields = fields;
        this.vectors = new OffHeapColumnVector[types.length];
        this.released = new boolean[types.length];
        int metaSize = 0;
        for (int i = 0; i < types.length; i++) {
            vectors[i] = new OffHeapColumnVector(capacity, types[i]);
            metaSize += types[i].computeColumnSize();
            released[i] = false;
        }
        this.meta = new OffHeapColumnVector(metaSize, new ColumnType("#meta", ColumnType.TypeValue.LONG));
        this.numRows = 0;
    }

    public void appendData(int fieldId, ColumnValue o) {
        vectors[fieldId].appendValue(o);
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

    public int getNumRows() {
        return this.numRows;
    }

    public long getMetaNativeAddress() {
        meta.appendLong(numRows);
        for (OffHeapColumnVector v : vectors) {
            v.updateMeta(meta);
        }
        return meta.valuesNativeAddress();
    }

    public void close() {
        for (int i = 0; i < vectors.length; i++) {
            releaseOffHeapColumnVector(i);
        }
        meta.close();
    }

    // for test only.
    public String dump(int rowLimit) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowLimit && i < numRows; i++) {
            sb.append("row" + i + ": [");
            for (int j = 0; j < vectors.length; j++) {
                if (j != 0) {
                    sb.append(',');
                }
                sb.append(fields[j]).append(':');
                vectors[j].dump(sb, i);
            }
            sb.append("]\n");
        }
        return sb.toString();
    }

    public void show(int rowLimit) {
        System.out.println("OffHeapTable: numRows = " + numRows);
        System.out.println(dump(rowLimit));
    }

    // for test only.
    public static class MetaChecker {
        private OffHeapColumnVector meta;
        int offset;
        boolean verbose;

        public MetaChecker(OffHeapColumnVector meta, int offset) {
            this.meta = meta;
            this.offset = offset;
        }

        public void setVerbose(boolean v) {
            verbose = v;
        }

        public void check(String context, long expected) {
            if (meta.getLong(offset) != expected) {
                throw new RuntimeException(
                        "meta check failed at offset: " + offset + ", current = " + meta.getLong(offset) +
                                ", expected = " +
                                expected + ", context = " + context);
            }
            if (verbose) {
                System.out.println("meta check ok: " + context);
            }
            offset += 1;
        }
    }

    // for test only.
    public void checkTableMeta(boolean verbose) {
        MetaChecker checker = new MetaChecker(meta, 0);
        checker.setVerbose(verbose);
        checker.check("numRows", numRows);
        for (OffHeapColumnVector c : vectors) {
            c.checkMeta(checker);
        }
    }

}
