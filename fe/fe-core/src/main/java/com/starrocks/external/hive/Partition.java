// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.starrocks.external.hive.text.TextFileFormatDesc;

import java.util.Map;
import java.util.Objects;

public class Partition {
    private final Map<String, String> parameters;
    private final RemoteFileInputFormat inputFormat;
    private final TextFileFormatDesc textFileFormatDesc;
    private final String fullPath;
    private final boolean isSplittable;

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
}
