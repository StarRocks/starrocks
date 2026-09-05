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

package com.starrocks.persist.gson.internal;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/*
 *
 * The json adapter for Guava Table.
 * Current support:
 * 1. HashBasedTable
 *
 * The RowKey, ColumnKey and Value classes in Table should also be serializable.
 *
 * What is Adapter and Why we should implement it?
 *
 * Adapter is mainly used to provide serialization and deserialization methods for some complex classes.
 * Complex classes here usually refer to classes that are complex and cannot be modified.
 * These classes mainly include third-party library classes or some inherited classes.
 */

public class GuavaTableAdapter<R, C, V>
        implements JsonSerializer<Table<R, C, V>>, JsonDeserializer<Table<R, C, V>> {
    /*
     * serialize Table<R, C, V> as:
     * {
     * "rowKeys": [ "rowKey1", "rowKey2", ...],
     * "columnKeys": [ "colKey1", "colKey2", ...],
     * "cells" : [[0, 0, value1], [0, 1, value2], ...]
     * }
     *
     * the [0, 0] .. in cells are the indexes of rowKeys array and columnKeys array.
     * This serialization method can reduce the size of json string because it
     * replace the same row key
     * and column key to integer.
     */
    @Override
    public JsonElement serialize(Table<R, C, V> src, Type typeOfSrc, JsonSerializationContext context) {
        JsonArray rowKeysJsonArray = new JsonArray();
        Map<R, Integer> rowKeyToIndex = new HashMap<>();
        for (R rowKey : src.rowKeySet()) {
            rowKeyToIndex.put(rowKey, rowKeyToIndex.size());
            rowKeysJsonArray.add(context.serialize(rowKey));
        }
        JsonArray columnKeysJsonArray = new JsonArray();
        Map<C, Integer> columnKeyToIndex = new HashMap<>();
        for (C columnKey : src.columnKeySet()) {
            columnKeyToIndex.put(columnKey, columnKeyToIndex.size());
            columnKeysJsonArray.add(context.serialize(columnKey));
        }
        JsonArray cellsJsonArray = new JsonArray();
        for (Table.Cell<R, C, V> cell : src.cellSet()) {
            int rowIndex = rowKeyToIndex.get(cell.getRowKey());
            int columnIndex = columnKeyToIndex.get(cell.getColumnKey());
            cellsJsonArray.add(rowIndex);
            cellsJsonArray.add(columnIndex);
            cellsJsonArray.add(context.serialize(cell.getValue()));
        }
        JsonObject tableJsonObject = new JsonObject();
        tableJsonObject.addProperty("clazz", src.getClass().getSimpleName());
        tableJsonObject.add("rowKeys", rowKeysJsonArray);
        tableJsonObject.add("columnKeys", columnKeysJsonArray);
        tableJsonObject.add("cells", cellsJsonArray);
        return tableJsonObject;
    }

    @Override
    public Table<R, C, V> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
        Type typeOfR;
        Type typeOfC;
        Type typeOfV;
        {
            ParameterizedType parameterizedType = (ParameterizedType) typeOfT;
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            typeOfR = actualTypeArguments[0];
            typeOfC = actualTypeArguments[1];
            typeOfV = actualTypeArguments[2];
        }
        JsonObject tableJsonObject = json.getAsJsonObject();
        String tableClazz = tableJsonObject.get("clazz").getAsString();
        JsonArray rowKeysJsonArray = tableJsonObject.getAsJsonArray("rowKeys");
        Map<Integer, R> rowIndexToKey = new HashMap<>();
        for (JsonElement jsonElement : rowKeysJsonArray) {
            R rowKey = context.deserialize(jsonElement, typeOfR);
            rowIndexToKey.put(rowIndexToKey.size(), rowKey);
        }
        JsonArray columnKeysJsonArray = tableJsonObject.getAsJsonArray("columnKeys");
        Map<Integer, C> columnIndexToKey = new HashMap<>();
        for (JsonElement jsonElement : columnKeysJsonArray) {
            C columnKey = context.deserialize(jsonElement, typeOfC);
            columnIndexToKey.put(columnIndexToKey.size(), columnKey);
        }
        JsonArray cellsJsonArray = tableJsonObject.getAsJsonArray("cells");
        Table<R, C, V> table = null;
        switch (tableClazz) {
            case "HashBasedTable":
                table = HashBasedTable.create();
                break;
            default:
                Preconditions.checkState(false, "unknown guava table class: " + tableClazz);
                break;
        }
        for (int i = 0; i < cellsJsonArray.size(); i = i + 3) {
            // format is [rowIndex, columnIndex, value]
            int rowIndex = cellsJsonArray.get(i).getAsInt();
            int columnIndex = cellsJsonArray.get(i + 1).getAsInt();
            R rowKey = rowIndexToKey.get(rowIndex);
            C columnKey = columnIndexToKey.get(columnIndex);
            V value = context.deserialize(cellsJsonArray.get(i + 2), typeOfV);
            table.put(rowKey, columnKey, value);
        }
        return table;
    }
}

