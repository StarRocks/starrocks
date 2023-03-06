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


package com.starrocks.connector.hive;

import com.starrocks.connector.PartitionInfo;

import java.util.Map;
import java.util.Objects;

/**
 * Partition stores some necessary information used in the planner stage
 * such as in the cbo and building scan range stage. The purpose of caching partition instance
 * is to reduce repeated calls to the hive metastore rpc interface at each stage.
 */
public class Partition implements PartitionInfo {
    private final Map<String, String> parameters;
    private final RemoteFileInputFormat inputFormat;
    private final TextFileFormatDesc textFileFormatDesc;
    private final String fullPath;
    private final boolean isSplittable;

    public static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

    public Partition(Map<String, String> parameters,
                     RemoteFileInputFormat inputFormat,
                     TextFileFormatDesc textFileFormatDesc,
                     String fullPath,
                     boolean isSplittable) {
        this.parameters = parameters;
        this.inputFormat = inputFormat;
        this.textFileFormatDesc = textFileFormatDesc;
        this.fullPath = fullPath;
        this.isSplittable = isSplittable;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public RemoteFileInputFormat getInputFormat() {
        return inputFormat;
    }

    public TextFileFormatDesc getTextFileFormatDesc() {
        return textFileFormatDesc;
    }

    public String getFullPath() {
        return fullPath;
    }

    public boolean isSplittable() {
        return isSplittable;
    }

    @Override
    public long getModifiedTime() {
        return Long.parseLong(parameters.get(TRANSIENT_LAST_DDL_TIME));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Partition partition = (Partition) o;

        return isSplittable == partition.isSplittable &&
                Objects.equals(parameters, partition.parameters) &&
                inputFormat == partition.inputFormat &&
                Objects.equals(textFileFormatDesc, partition.textFileFormatDesc) &&
                Objects.equals(fullPath, partition.fullPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters, inputFormat, textFileFormatDesc, fullPath, isSplittable);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Partition{");
        sb.append("parameters=").append(parameters);
        sb.append(", inputFormat=").append(inputFormat);
        sb.append(", textFileFormatDesc=").append(textFileFormatDesc);
        sb.append(", fullPath='").append(fullPath).append('\'');
        sb.append(", isSplittable=").append(isSplittable);
        sb.append('}');
        return sb.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Map<String, String> params;
        private RemoteFileInputFormat inputFormat;
        private TextFileFormatDesc textFileFormatDesc;
        private String fullPath;
        private boolean isSplittable;

        public Builder() {
        }

        public Builder setParams(Map<String, String> params) {
            this.params = params;
            return this;
        }

        public Builder setInputFormat(RemoteFileInputFormat inputFormat) {
            this.inputFormat = inputFormat;
            return this;
        }

        public Builder setTextFileFormatDesc(TextFileFormatDesc textFileFormatDesc) {
            this.textFileFormatDesc = textFileFormatDesc;
            return this;
        }

        public Builder setFullPath(String fullPath) {
            this.fullPath = fullPath;
            return this;
        }

        public Builder setSplittable(boolean splittable) {
            isSplittable = splittable;
            return this;
        }

        public Partition build() {
            return new Partition(params, inputFormat, textFileFormatDesc, fullPath, isSplittable);
        }
    }
}
