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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

// Now only support utf-8
public class SetNamesVar extends SetListItem {
    public static final String DEFAULT_NAMES = "utf8";
    public static final String GBK_NAMES = "gbk";
    private String charset;
    private final String collate;

    public SetNamesVar(String charsetName) {
        this(charsetName, null);
    }

    public SetNamesVar(String charsetName, String collate) {
        this(charsetName, collate, NodePosition.ZERO);
    }

    public SetNamesVar(String charsetName, String collate, NodePosition pos) {
        super(pos);
        this.charset = charsetName;
        this.collate = collate;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getCollate() {
        if (collate == null) {
            return "DEFAULT";
        } else {
            return collate;
        }
    }
}
