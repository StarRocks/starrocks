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

package com.starrocks.connector.iceberg;

import com.starrocks.jni.connector.ColumnValue;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class IcebergPropertiesTableScanner extends AbstractIcebergMetadataScanner {
    private Iterator<Map.Entry<String, String>> reader;

    public IcebergPropertiesTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
    }

    @Override
    public void doOpen() {
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        for (; numRows < getTableSize(); numRows++) {
            if (!reader.hasNext()) {
                break;
            }
            Map.Entry<String, String> propertyEntry = reader.next();
            String key = propertyEntry.getKey();
            String value = propertyEntry.getValue();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = get(requiredFields[i], key, value);
                if (fieldData == null) {
                    appendData(i, null);
                } else {
                    ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData, timezone);
                    appendData(i, fieldValue);
                }
            }
        }
        return numRows;
    }

    @Override
    public void doClose() {
        reader = null;
    }

    @Override
    protected void initReader() {
        Map<String, String> allProperties = new LinkedHashMap<>();
        allProperties.put("location", table.location());
        allProperties.putAll(table.properties());
        reader = allProperties.entrySet().iterator();
    }

    private Object get(String columnName, String key, String value) {
        switch (columnName) {
            case "key":
                return key;
            case "value":
                return value;
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }
}
