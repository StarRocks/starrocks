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

import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.utils.FileStatus;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.Map;

// FileScanTask represents one `AddFile` with DV in DeltaLake.
// TODO: The file representations of different Catalogs will be unified later.
public class FileScanTask {
    private final FileStatus fileStatus;
    private final long records;
    private final Map<String, String> partitionValues;
    private final DeletionVectorDescriptor dv;

    public FileScanTask(FileStatus fileStatus, long records, Map<String, String> partitionValues,
                        DeletionVectorDescriptor dv) {
        this.fileStatus = fileStatus;
        this.records = records;
        this.partitionValues = new CaseInsensitiveMap<>(partitionValues);
        this.dv = dv;
    }

    public FileStatus getFileStatus() {
        return this.fileStatus;
    }

    public long getFileSize() {
        return this.fileStatus.getSize();
    }

    public long getRecords() {
        return this.records;
    }

    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    public DeletionVectorDescriptor getDv() {
        return dv;
    }
}
