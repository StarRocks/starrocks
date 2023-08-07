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

import java.util.List;

public interface ColumnValue {
    boolean getBoolean();

    short getShort();

    int getInt();

    float getFloat();

    long getLong();

    double getDouble();

    String getString(ColumnType.TypeValue type);
    String getTimestamp(ColumnType.TypeValue type);
    byte[] getBytes();

    void unpackArray(List<ColumnValue> values);

    void unpackMap(List<ColumnValue> keys, List<ColumnValue> values);

    void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values);
}
