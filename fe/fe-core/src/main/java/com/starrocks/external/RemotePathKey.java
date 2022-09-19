// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import java.util.Objects;

public class RemotePathKey {
    private final String path;
    private final boolean isRecursive;

    public static RemotePathKey of(String path, boolean isRecursive) {
        return new RemotePathKey(path, isRecursive);
    }

    public RemotePathKey(String path, boolean isRecursive) {
        this.path = path;
        this.isRecursive = isRecursive;
    }

    public String getPath() {
        return path;
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
        RemotePathKey that = (RemotePathKey) o;
        return isRecursive == that.isRecursive && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, isRecursive);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("RemotePathKey{");
        sb.append("path='").append(path).append('\'');
        sb.append(", isRecursive=").append(isRecursive);
        sb.append('}');
        return sb.toString();
    }
}
