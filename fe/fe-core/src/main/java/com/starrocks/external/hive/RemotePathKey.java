package com.starrocks.external.hive;

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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemotePathKey that = (RemotePathKey) o;
        return isRecursive == that.isRecursive && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, isRecursive);
    }
}
