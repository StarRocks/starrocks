// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.starrocks.external.hive.RemoteFileInputFormat;

import java.util.List;

public class RemoteFileInfo {
    private RemoteFileInputFormat format;
    private List<RemoteFileDesc> files;
    private final String fullPath;

    public RemoteFileInfo(RemoteFileInputFormat format, List<RemoteFileDesc> files, String fullPath) {
        this.format = format;
        this.files = files;
        this.fullPath = fullPath;
    }

    public RemoteFileInputFormat getFormat() {
        return format;
    }

    public void setFormat(RemoteFileInputFormat format) {
        this.format = format;
    }

    public List<RemoteFileDesc> getFiles() {
        return files;
    }

    public void setFiles(List<RemoteFileDesc> files) {
        this.files = files;
    }

    public String getFullPath() {
        return fullPath;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private RemoteFileInputFormat format;
        private List<RemoteFileDesc> files;
        private String fullPath;

        public Builder setFormat(RemoteFileInputFormat format) {
            this.format = format;
            return this;
        }

        public Builder setFiles(List<RemoteFileDesc> files) {
            this.files = files;
            return this;
        }

        public Builder setFullPath(String fullPath) {
            this.fullPath = fullPath;
            return this;
        }

        public RemoteFileInfo build() {
            return new RemoteFileInfo(format, files, fullPath);
        }
    }
}
