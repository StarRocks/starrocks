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

<<<<<<< HEAD
=======
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.List;

public interface ColumnValue {
    boolean getBoolean();

    short getShort();

    int getInt();

    float getFloat();

    long getLong();

    double getDouble();

    String getString(ColumnType.TypeValue type);
<<<<<<< HEAD
    String getTimestamp(ColumnType.TypeValue type);
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    byte[] getBytes();

    void unpackArray(List<ColumnValue> values);

    void unpackMap(List<ColumnValue> keys, List<ColumnValue> values);

    void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values);

    byte getByte();
<<<<<<< HEAD
=======

    BigDecimal getDecimal();

    LocalDate getDate();

    LocalDateTime getDateTime(ColumnType.TypeValue type);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}
