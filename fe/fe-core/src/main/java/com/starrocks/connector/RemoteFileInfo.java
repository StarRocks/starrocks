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

import com.starrocks.connector.hive.RemoteFileInputFormat;

import java.util.ArrayList;
import java.util.List;

public class RemoteFileInfo {
    private RemoteFileInputFormat format;
    private List<RemoteFileDesc> files = new ArrayList<>();
    private String fullPath;

    public RemoteFileInfo(RemoteFileInputFormat format, List<RemoteFileDesc> files, String fullPath) {
        this.format = format;
        this.files = files;
        this.fullPath = fullPath;
    }

    public RemoteFileInfo() {
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemoteFileInfo{");
        sb.append("format=").append(format);
        sb.append(", files=").append(files);
        sb.append(", fullPath='").append(fullPath).append('\'');
        sb.append('}');
        return sb.toString();
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
