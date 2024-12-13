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

<<<<<<< HEAD
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
=======
import java.util.Objects;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

public class RemotePathKey {
    private final String path;
    private final boolean isRecursive;
<<<<<<< HEAD

    // The table location must exist in HudiTable
    private final Optional<String> hudiTableLocation;

    public static class HudiContext {
        // ---- concurrent initialization -----
        public ReentrantLock lock = new ReentrantLock();
        public int usedCount = 0;
        // ---- actual fields -----
        public HoodieTableFileSystemView fsView = null;
        public HoodieTimeline timeline = null;
        public HoodieInstant lastInstant = null;

        public void close() {
            if (fsView != null) {
                fsView.close();
                fsView = null;
                timeline = null;
                lastInstant = null;
            }
        }
    }

    private HudiContext hudiContext;

    public static RemotePathKey of(String path, boolean isRecursive) {
        return new RemotePathKey(path, isRecursive, Optional.empty());
    }

    public static RemotePathKey of(String path, boolean isRecursive, Optional<String> hudiTableLocation) {
        return new RemotePathKey(path, isRecursive, hudiTableLocation);
    }

    public RemotePathKey(String path, boolean isRecursive, Optional<String> hudiTableLocation) {
        this.path = path;
        this.isRecursive = isRecursive;
        this.hudiTableLocation = hudiTableLocation;
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public boolean approximateMatchPath(String basePath, boolean isRecursive) {
        String pathWithSlash = path.endsWith("/") ? path : path + "/";
        String basePathWithSlash = basePath.endsWith("/") ? basePath : basePath + "/";
        return pathWithSlash.startsWith(basePathWithSlash) && (this.isRecursive == isRecursive);
    }

    public String getPath() {
        return path;
    }

<<<<<<< HEAD
    public boolean isRecursive() {
        return isRecursive;
    }

    public Optional<String> getHudiTableLocation() {
        return hudiTableLocation;
=======
    public String getTableLocation() {
        return tableLocation;
    }

    public boolean isRecursive() {
        return isRecursive;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
                Objects.equals(path, pathKey.path) &&
                Objects.equals(hudiTableLocation, pathKey.hudiTableLocation);
=======
                Objects.equals(path, pathKey.path);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public int hashCode() {
<<<<<<< HEAD
        return Objects.hash(path, isRecursive, hudiTableLocation);
=======
        return Objects.hash(path, isRecursive);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemotePathKey{");
        sb.append("path='").append(path).append('\'');
        sb.append(", isRecursive=").append(isRecursive);
<<<<<<< HEAD
        if (hudiTableLocation.isPresent()) {
            sb.append(", hudiTableLocation=").append(hudiTableLocation);
        }
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        sb.append('}');
        return sb.toString();
    }

<<<<<<< HEAD
    public void setHudiContext(HudiContext ctx) {
        hudiContext = ctx;
    }

    public HudiContext getHudiContext() {
        return hudiContext;
=======
    public void setScanContext(RemoteFileScanContext ctx) {
        scanContext = ctx;
        tableLocation = ctx.tableLocation;
    }

    public RemoteFileScanContext getScanContext() {
        return scanContext;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
}
