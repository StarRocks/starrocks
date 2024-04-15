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
package com.starrocks.connector.delta.cost;

public class DeltaLakeFileStats {

    private long recordCount;
    private long fileCount;
    private long size;

    public DeltaLakeFileStats() {
        this.recordCount = 0;
        this.fileCount = 0;
        this.size = 0;
    }

    public void incrementRecordCount(long count) {
        this.recordCount += count;
    }

    public void incrementSize(long count) {
        this.size += count;
    }

    public void incrementFileCount(long count) {
        this.fileCount += count;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getFileCount() {
        return fileCount;
    }

    public long getSize() {
        return size;
    }

}
