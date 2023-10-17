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
        if (fieldDelim != null) {
            desc.setField_delim(fieldDelim);
        }
        if (lineDelim != null) {
            desc.setLine_delim(lineDelim);
        }
        if (collectionDelim != null) {
            desc.setCollection_delim(collectionDelim);
        }
        if (mapkeyDelim != null) {
            desc.setMapkey_delim(mapkeyDelim);
        }
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
