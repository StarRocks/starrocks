// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive.text;

import com.starrocks.thrift.TTextFileDesc;

public class TextFileFormatDesc {
    private String fieldDelim;
    private String lineDelim;

    public TextFileFormatDesc(String fDelim, String lDelim) {
        this.fieldDelim = fDelim;
        this.lineDelim = lDelim;
    }

    public TTextFileDesc toThrift() {
        TTextFileDesc desc = new TTextFileDesc();
        desc.field_delim = fieldDelim;
        desc.line_delim = lineDelim;
        return desc;
    }
}
