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

import io.delta.kernel.utils.FileStatus;

import java.util.Objects;

public class DeltaLakeFileStatus {
    private final String path;
    private final long size;
    private final long modificationTime;

    protected DeltaLakeFileStatus(String path, long size, long modificationTime) {
        this.path = path;
        this.size = size;
        this.modificationTime = modificationTime;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaLakeFileStatus that = (DeltaLakeFileStatus) o;
        return size == that.size && modificationTime == that.modificationTime && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, size, modificationTime);
    }

    public static DeltaLakeFileStatus of(FileStatus fileStatus) {
        return new DeltaLakeFileStatus(fileStatus.getPath(), fileStatus.getSize(), fileStatus.getModificationTime());
    }
}
