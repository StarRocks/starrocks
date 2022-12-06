// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.starrocks.thrift.TTextFileDesc;

public class TextFileFormatDesc {

    private final String fieldDelim;

    private final String lineDelim;

    // Control hive array's element delimiter.
    private final String collectionDelim;

    // mapkey_delimiter is the separator between key and value in map.
    // For example, {"smith": age} mapkey_delimiter is ':'.
    private final String mapkeyDelim;

    public TextFileFormatDesc(String fieldDelim, String lineDelim, String collectionDelim, String mapkeyDelim) {
        this.fieldDelim = fieldDelim;
        this.lineDelim = lineDelim;
        this.collectionDelim = collectionDelim;
        this.mapkeyDelim = mapkeyDelim;
    }

    public String getFieldDelim() {
        return fieldDelim;
    }

    public String getLineDelim() {
        return lineDelim;
    }

    public String getCollectionDelim() {
        return collectionDelim;
    }

    public String getMapkeyDelim() {
        return mapkeyDelim;
    }

    public TTextFileDesc toThrift() {
        TTextFileDesc desc = new TTextFileDesc();
        desc.field_delim = fieldDelim;
        desc.line_delim = lineDelim;
        desc.collection_delim = collectionDelim;
        desc.mapkey_delim = mapkeyDelim;
        return desc;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("TextFileFormatDesc{");
        sb.append("fieldDelim='").append(fieldDelim).append('\'');
        sb.append(", lineDelim='").append(lineDelim).append('\'');
        sb.append(", collectionDelim='").append(collectionDelim).append('\'');
        sb.append(", mapkeyDelim='").append(mapkeyDelim).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
