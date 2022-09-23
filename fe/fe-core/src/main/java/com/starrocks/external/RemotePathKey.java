// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

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
        sb.append(", hudiTableLocation=").append(hudiTableLocation);
        sb.append('}');
        return sb.toString();
    }
}
