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

import java.io.IOException;

/**
 * The parent class of JNI scanner, developers need to inherit this class and implement the following methods:
 * 1. {@link ConnectorScanner#open()}
 * 2. {@link ConnectorScanner#close()}
 * 3. {@link ConnectorScanner#getNext()}
 * <p>
 * The constructor of inherited subclasses need to accept the following parameters in order:
 * 1. int: the chunk size
 * 2. Map<String, String>: the custom parameters
 * <p>
 * {@link ConnectorScanner#initOffHeapTableWriter(ColumnType[], String[], int)} need be called to initialize
 * {@link ConnectorScanner#tableSize} and {@link ConnectorScanner#types}
 * before calling {@link ConnectorScanner#getNext()} (maybe in constructor or {@link ConnectorScanner#open()})
 * <p>
 * BE will call these methods as follows (described in pseudocode):
 * open();
 * do {
 * int rows = getNext();
 * // do something...
 * if (rows == 0) {
 * break;
 * }
 * } while (true);
 * close();
 */
public abstract class ConnectorScanner {
    private OffHeapTable offHeapTable;
    private String[] fields;
    private ColumnType[] types;
    private int tableSize;

    /**
     * Initialize the reader with parameters passed by the class constructor and allocate necessary resources.
     * Developers can call {@link ConnectorScanner#initOffHeapTableWriter(ColumnType[], String[], int)} method here
     * to allocate memory spaces.
     */
    public abstract void open() throws IOException;

    /**
     * Close the reader and release resources.
     */
    public abstract void close() throws IOException;

    /**
     * Scan original data and save it to off-heap table.
     *
     * @return The number of rows scanned.
     * The specific implementation needs to call the {@link ConnectorScanner#appendData(int, Object)} method
     * to save data to off-heap table.
     * The number of rows scanned must less than or equal to {@link ConnectorScanner#tableSize}
     */
    public abstract int getNext() throws IOException;

    /**
     * This method need be called before {@link ConnectorScanner#getNext()}
     *
     * @param requiredTypes  column types to scan
     * @param requiredFields columns names to scan
     * @param fetchSize      number of rows
     */
    protected void initOffHeapTableWriter(ColumnType[] requiredTypes, String[] requiredFields, int fetchSize) {
        this.tableSize = fetchSize;
        this.types = requiredTypes;
        this.fields = requiredFields;
    }

    protected void appendData(int index, ColumnValue value) {
        offHeapTable.appendData(index, value);
    }

    protected int getTableSize() {
        return tableSize;
    }

    public OffHeapTable getOffHeapTable() {
        return offHeapTable;
    }

    public long getNextOffHeapChunk() throws IOException {
        initOffHeapTable();
        int numRows = 0;
        try {
            numRows = getNext();
        } catch (IOException e) {
            releaseOffHeapTable();
            throw e;
        }
        return finishOffHeapTable(numRows);
    }

    private void initOffHeapTable() {
        offHeapTable = new OffHeapTable(types, fields, tableSize);
    }

    private long finishOffHeapTable(int numRows) {
        offHeapTable.setNumRows(numRows);
        return offHeapTable.getMetaNativeAddress();
    }

    protected void releaseOffHeapColumnVector(int fieldId) {
        offHeapTable.releaseOffHeapColumnVector(fieldId);
    }

    protected void releaseOffHeapTable() {
        if (offHeapTable != null) {
            offHeapTable.close();
        }
    }
}
