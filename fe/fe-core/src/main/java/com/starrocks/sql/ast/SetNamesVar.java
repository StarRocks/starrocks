// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

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

    public String getCollate() {
        if (collate == null) {
            return "DEFAULT";
        } else {
            return collate;
        }
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

        if (!charset.equalsIgnoreCase(DEFAULT_NAMES) && !charset.equalsIgnoreCase(GBK_NAMES)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_CHARACTER_SET, charset);
        }
        // be is not supported yet,so Display unsupported information to the user
        if (!charset.equalsIgnoreCase(DEFAULT_NAMES)) {
            throw new SemanticException("charset name " + charset + " is not supported yet");
        }
    }
}
