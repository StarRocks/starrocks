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
import java.util.Optional;

public class RemotePathKey {
    private final String path;
    private final boolean isRecursive;

    // The table location must exist in HudiTable
    private final Optional<String> hudiTableLocation;

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
    }

    public boolean approximateMatchPath(String basePath, boolean isRecursive) {
        String pathWithSlash = path.endsWith("/") ? path : path + "/";
        String basePathWithSlash =  basePath.endsWith("/") ? basePath : basePath + "/";
        return pathWithSlash.startsWith(basePathWithSlash) && (this.isRecursive == isRecursive);
    }

    public String getPath() {
        return path;
    }

    public boolean isRecursive() {
        return isRecursive;
    }

    public Optional<String> getHudiTableLocation() {
        return hudiTableLocation;
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
                Objects.equals(path, pathKey.path) &&
                Objects.equals(hudiTableLocation, pathKey.hudiTableLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, isRecursive, hudiTableLocation);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemotePathKey{");
        sb.append("path='").append(path).append('\'');
        sb.append(", isRecursive=").append(isRecursive);
        if (hudiTableLocation.isPresent()) {
            sb.append(", hudiTableLocation=").append(hudiTableLocation);
        }
        sb.append('}');
        return sb.toString();
    }
}
