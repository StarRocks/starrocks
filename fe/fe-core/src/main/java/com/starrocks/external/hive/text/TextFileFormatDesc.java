// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive.text;

import com.starrocks.thrift.TTextFileDesc;

public class TextFileFormatDesc {

    private String fieldDelim;

    private String lineDelim;

    private String collectionDelim;

    private String mapkeyDelim;

    public TextFileFormatDesc(String fieldDelim, String lineDelim, String collectionDelim, String mapkeyDelim) {
        this.fieldDelim = fieldDelim;
        this.lineDelim = lineDelim;
        this.collectionDelim = collectionDelim;
        this.mapkeyDelim = mapkeyDelim;
    }

    public TTextFileDesc toThrift() {
        TTextFileDesc desc = new TTextFileDesc();
        desc.field_delim = fieldDelim;
        desc.line_delim = lineDelim;
        desc.collection_delim = collectionDelim;
        desc.mapkey_delim = mapkeyDelim;
        return desc;
    }
}
