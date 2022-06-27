// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SetNamesVar.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.sql.analyzer.SemanticException;

// Now only support utf-8
public class SetNamesVar extends SetVar {
    private static final String DEFAULT_NAMES = "utf8";
    private static final String GBK_NAMES = "gbk";
    private String charset;
    private String collate;

    public SetNamesVar(String charsetName) {
        this(charsetName, null);
    }

    public SetNamesVar(String charsetName, String collate) {
        this.charset = charsetName;
        this.collate = collate;
    }

    public String getCharset() {
        return charset;
    }

    @Override
    public void analyze() {
        if (Strings.isNullOrEmpty(charset)) {
            charset = DEFAULT_NAMES;
        } else {
            charset = charset.toLowerCase();
        }
        // utf8-superset transform to utf8
        if (charset.startsWith(DEFAULT_NAMES)) {
            charset = DEFAULT_NAMES;
        }

        if (!charset.equalsIgnoreCase(DEFAULT_NAMES)&&!charset.equalsIgnoreCase(GBK_NAMES)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_CHARACTER_SET, charset);
        }
        // be is not supported yet,so Display unsupported information to the user
        if (!charset.equalsIgnoreCase(DEFAULT_NAMES)){
            throw new SemanticException("charset name " + charset + " is not supported yet");
        }
    }

    @Override
    public String toSql() {
        return "NAMES '" + charset + "' COLLATE "
                + (Strings.isNullOrEmpty(collate) ? "DEFAULT" : "'" + collate.toLowerCase() + "'");
    }

    @Override
    public String toString() {
        return toSql();
    }
}
