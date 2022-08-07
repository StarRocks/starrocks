package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class RemoteFileInfo {
    private RemoteFileInputFormat format;
    private List<RemoteFileDesc> files;
    private String fullPath;

    public RemoteFileInfo(RemoteFileInputFormat format, List<RemoteFileDesc> files, String fullPath) {
        this.format = format;
        this.files = files;
        this.fullPath = fullPath;
    }

    public RemoteFileInputFormat getFormat() {
        return format;
    }

    public List<RemoteFileDesc> getFiles() {
        return files;
    }

    public void setFiles(ImmutableList<RemoteFileDesc> files) {
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
