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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;

import java.util.List;

public class IcebergSplitScanTask implements FileScanTask {
    private final long len;
    private final long offset;
    private final FileScanTask fileScanTask;

    public IcebergSplitScanTask(long offset, long len, FileScanTask fileScanTask) {
        this.offset = offset;
        this.len = len;
        this.fileScanTask = fileScanTask;
    }

    @Override
    public DataFile file() {
        return fileScanTask.file();
    }

    @Override
    public List<DeleteFile> deletes() {
        return fileScanTask.deletes();
    }

    @Override
    public PartitionSpec spec() {
        return fileScanTask.spec();
    }

    @Override
    public long start() {
        return offset;
    }

    @Override
    public long length() {
        return len;
    }

    @Override
    public Expression residual() {
        return fileScanTask.residual();
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
        throw new UnsupportedOperationException("Cannot split a task which is already split");
    }
}
