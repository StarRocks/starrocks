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

package com.starrocks.connector.delta;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.types.StructType;

import static io.delta.kernel.internal.InternalScanFileUtils.SCAN_FILE_SCHEMA;
import static io.delta.kernel.internal.replay.LogReplay.ADD_FILE_PATH_ORDINAL;

public class DeltaLakeTestBase {
    public static final StructType ADD_FILE_SCHEMA =
            (StructType) SCAN_FILE_SCHEMA.get("add").getDataType();

    public static final int ADD_FILE_SIZE_ORDINAL = ADD_FILE_SCHEMA.indexOf("size");

    public static final int ADD_FILE_MOD_TIME_ORDINAL =
            ADD_FILE_SCHEMA.indexOf("modificationTime");


    public static void getAddFilePath(ColumnVector addFileVector, int rowId) {
        addFileVector.getChild(ADD_FILE_PATH_ORDINAL).getString(rowId);
    }

    public static Row getAddFileEntry(Row scanFileInfo) {
        if (scanFileInfo.isNullAt(InternalScanFileUtils.ADD_FILE_ORDINAL)) {
            throw new IllegalArgumentException("There is no `add` entry in the scan file row");
        }
        return scanFileInfo.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL);
    }
}
