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

package com.starrocks.connector;

import java.util.Objects;

public class RemotePathKey {
    private final String path;
    private final boolean isRecursive;
    private RemoteFileScanContext scanContext;
    private String tableLocation;

    public static RemotePathKey of(String path, boolean isRecursive) {
        return new RemotePathKey(path, isRecursive);
    }

    public RemotePathKey(String path, boolean isRecursive) {
        this.path = path;
        this.isRecursive = isRecursive;
        this.scanContext = null;
        this.tableLocation = null;
    }

    public boolean approximateMatchPath(String basePath, boolean isRecursive) {
        String pathWithSlash = path.endsWith("/") ? path : path + "/";
        String basePathWithSlash = basePath.endsWith("/") ? basePath : basePath + "/";
        return pathWithSlash.startsWith(basePathWithSlash) && (this.isRecursive == isRecursive);
    }

    public String getPath() {
        return path;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public boolean isRecursive() {
        return isRecursive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemotePathKey pathKey = (RemotePathKey) o;
        return isRecursive == pathKey.isRecursive &&
                Objects.equals(path, pathKey.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, isRecursive);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemotePathKey{");
        sb.append("path='").append(path).append('\'');
        sb.append(", isRecursive=").append(isRecursive);
        sb.append('}');
        return sb.toString();
    }

    public void drop() {
        if (scanContext != null) {
            scanContext = null;
        }
    }

    public void setScanContext(RemoteFileScanContext ctx) {
        scanContext = ctx;
        tableLocation = ctx.tableLocation;
    }

    public RemoteFileScanContext getScanContext() {
        return scanContext;
    }
}
