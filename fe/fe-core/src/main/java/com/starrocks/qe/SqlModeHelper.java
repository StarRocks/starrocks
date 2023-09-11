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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/SqlModeHelper.java

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

package com.starrocks.qe;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlModeHelper {
    private static final Logger LOG = LogManager.getLogger(SqlModeHelper.class);

    // TODO(xuyang): these mode types are copy from MYSQL mode types, which are example
    //  of how they works and to be compatible with MySQL, so for now they are not
    //  really meaningful.

    /* When a new session is create, its sql mode is set to MODE_DEFAULT */
    public static final long MODE_DEFAULT = 32L;
    /* Bits for different SQL MODE modes, you can add custom SQL MODE here */
    public static final long MODE_REAL_AS_FLOAT = 1L;
    public static final long MODE_PIPES_AS_CONCAT = 1L << 1;
    public static final long MODE_ANSI_QUOTES = 1L << 2;
    public static final long MODE_IGNORE_SPACE = 1L << 3;
    public static final long MODE_NOT_USED = 1L << 4;
    public static final long MODE_ONLY_FULL_GROUP_BY = 1L << 5;
    public static final long MODE_NO_UNSIGNED_SUBTRACTION = 1L << 6;
    public static final long MODE_NO_DIR_IN_CREATE = 1L << 7;

    public static final long MODE_FORBID_INVALID_DATE = 1L << 8;
    public static final long MODE_ALLOW_THROW_EXCEPTION = 1L << 9;
    public static final long MODE_DOUBLE_LITERAL = 1L << 17;
    public static final long MODE_ANSI = 1L << 18;
    public static final long MODE_NO_AUTO_VALUE_ON_ZERO = 1L << 19;
    public static final long MODE_NO_BACKSLASH_ESCAPES = 1L << 20;
    public static final long MODE_STRICT_TRANS_TABLES = 1L << 21;
    public static final long MODE_STRICT_ALL_TABLES = 1L << 22;
    // NO_ZERO_IN_DATE and NO_ZERO_DATE are removed in mysql 5.7 and merged into STRICT MODE.
    // However, for backward compatibility during upgrade, these modes are kept.
    @Deprecated
    public static final long MODE_NO_ZERO_IN_DATE = 1L << 23;
    @Deprecated
    public static final long MODE_NO_ZERO_DATE = 1L << 24;
    public static final long MODE_INVALID_DATES = 1L << 25;
    public static final long MODE_ERROR_FOR_DIVISION_BY_ZERO = 1L << 26;

    public static final long MODE_TRADITIONAL = 1L << 27;
    public static final long MODE_HIGH_NOT_PRECEDENCE = 1L << 29;
    public static final long MODE_NO_ENGINE_SUBSTITUTION = 1L << 30;
    public static final long MODE_PAD_CHAR_TO_FULL_LENGTH = 1L << 31;
    public static final long MODE_TIME_TRUNCATE_FRACTIONAL = 1L << 32;
    public static final long MODE_SORT_NULLS_LAST = 1L << 33;
    public static final long MODE_LAST = 1L << 34;
    public static final long MODE_ERROR_IF_OVERFLOW = 1L << 35;

    public static final long MODE_ALLOWED_MASK =
            (MODE_REAL_AS_FLOAT | MODE_PIPES_AS_CONCAT | MODE_ANSI_QUOTES |
                    MODE_IGNORE_SPACE | MODE_NOT_USED | MODE_ONLY_FULL_GROUP_BY |
                    MODE_NO_UNSIGNED_SUBTRACTION | MODE_NO_DIR_IN_CREATE | MODE_DOUBLE_LITERAL |
                    MODE_FORBID_INVALID_DATE | MODE_ALLOW_THROW_EXCEPTION |
                    MODE_NO_AUTO_VALUE_ON_ZERO | MODE_NO_BACKSLASH_ESCAPES |
                    MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES | MODE_NO_ZERO_IN_DATE |
                    MODE_NO_ZERO_DATE | MODE_INVALID_DATES | MODE_ERROR_FOR_DIVISION_BY_ZERO |
                    MODE_HIGH_NOT_PRECEDENCE | MODE_NO_ENGINE_SUBSTITUTION |
                    MODE_PAD_CHAR_TO_FULL_LENGTH | MODE_TRADITIONAL | MODE_ANSI |
                    MODE_TIME_TRUNCATE_FRACTIONAL | MODE_SORT_NULLS_LAST) | MODE_ERROR_IF_OVERFLOW;

    private static final Map<String, Long> SQL_MODE_SET = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private static final Map<String, Long> COMBINE_MODE_SET = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    static {
        SQL_MODE_SET.put("REAL_AS_FLOAT", MODE_REAL_AS_FLOAT);
        SQL_MODE_SET.put("PIPES_AS_CONCAT", MODE_PIPES_AS_CONCAT);
        SQL_MODE_SET.put("ANSI_QUOTES", MODE_ANSI_QUOTES);
        SQL_MODE_SET.put("IGNORE_SPACE", MODE_IGNORE_SPACE);
        SQL_MODE_SET.put("NOT_USED", MODE_NOT_USED);
        SQL_MODE_SET.put("ONLY_FULL_GROUP_BY", MODE_ONLY_FULL_GROUP_BY);
        SQL_MODE_SET.put("NO_UNSIGNED_SUBTRACTION", MODE_NO_UNSIGNED_SUBTRACTION);
        SQL_MODE_SET.put("NO_DIR_IN_CREATE", MODE_NO_DIR_IN_CREATE);
        SQL_MODE_SET.put("ALLOW_THROW_EXCEPTION", MODE_ALLOW_THROW_EXCEPTION);
        SQL_MODE_SET.put("FORBID_INVALID_DATE", MODE_FORBID_INVALID_DATE);
        SQL_MODE_SET.put("MODE_DOUBLE_LITERAL", MODE_DOUBLE_LITERAL);
        SQL_MODE_SET.put("ANSI", MODE_ANSI);
        SQL_MODE_SET.put("NO_AUTO_VALUE_ON_ZERO", MODE_NO_AUTO_VALUE_ON_ZERO);
        SQL_MODE_SET.put("NO_BACKSLASH_ESCAPES", MODE_NO_BACKSLASH_ESCAPES);
        SQL_MODE_SET.put("STRICT_TRANS_TABLES", MODE_STRICT_TRANS_TABLES);
        SQL_MODE_SET.put("STRICT_ALL_TABLES", MODE_STRICT_ALL_TABLES);
        SQL_MODE_SET.put("NO_ZERO_IN_DATE", MODE_NO_ZERO_IN_DATE);
        SQL_MODE_SET.put("NO_ZERO_DATE", MODE_NO_ZERO_DATE);
        SQL_MODE_SET.put("INVALID_DATES", MODE_INVALID_DATES);
        SQL_MODE_SET.put("ERROR_FOR_DIVISION_BY_ZERO", MODE_ERROR_FOR_DIVISION_BY_ZERO);
        SQL_MODE_SET.put("TRADITIONAL", MODE_TRADITIONAL);
        SQL_MODE_SET.put("HIGH_NOT_PRECEDENCE", MODE_HIGH_NOT_PRECEDENCE);
        SQL_MODE_SET.put("NO_ENGINE_SUBSTITUTION", MODE_NO_ENGINE_SUBSTITUTION);
        SQL_MODE_SET.put("PAD_CHAR_TO_FULL_LENGTH", MODE_PAD_CHAR_TO_FULL_LENGTH);
        SQL_MODE_SET.put("TIME_TRUNCATE_FRACTIONAL", MODE_TIME_TRUNCATE_FRACTIONAL);
        SQL_MODE_SET.put("SORT_NULLS_LAST", MODE_SORT_NULLS_LAST);
        SQL_MODE_SET.put("ERROR_IF_OVERFLOW", MODE_ERROR_IF_OVERFLOW);

        COMBINE_MODE_SET.put("ANSI", (MODE_REAL_AS_FLOAT | MODE_PIPES_AS_CONCAT |
                MODE_ANSI_QUOTES | MODE_IGNORE_SPACE | MODE_ONLY_FULL_GROUP_BY));
        COMBINE_MODE_SET.put("TRADITIONAL", (MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES |
                MODE_NO_ZERO_IN_DATE | MODE_NO_ZERO_DATE | MODE_ERROR_FOR_DIVISION_BY_ZERO |
                MODE_NO_ENGINE_SUBSTITUTION));
    }

    // convert long type SQL MODE to string type that user can read
    public static String decode(Long sqlMode) throws DdlException {
        // 0 parse to empty string
        if (sqlMode == 0) {
            return "";
        }
        if ((sqlMode & ~MODE_ALLOWED_MASK) != 0) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.SQL_MODE, sqlMode);
        }

        List<String> names = new ArrayList<String>();
        for (Map.Entry<String, Long> mode : getSupportedSqlMode().entrySet()) {
            if ((sqlMode & mode.getValue()) != 0) {
                names.add(mode.getKey());
            }
        }

        return Joiner.on(',').join(names);
    }

    // convert string type SQL MODE to long type that session can store
    public static Long encode(String sqlMode) throws DdlException {
        List<String> names =
                Splitter.on(',').trimResults().omitEmptyStrings().splitToList(sqlMode);

        // empty string parse to 0
        long resultCode = 0L;
        for (String key : names) {
            long code = 0L;
            if (StringUtils.isNumeric(key)) {
                code |= expand(Long.valueOf(key));
            } else {
                code = getCodeFromString(key);
                if (code == 0) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.SQL_MODE, key);
                }
            }
            resultCode |= code;
            if ((resultCode & ~MODE_ALLOWED_MASK) != 0) {
                ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.SQL_MODE, key);
            }
        }
        return resultCode;
    }

    // expand the combine mode if exists
    public static long expand(long sqlMode) throws DdlException {
        for (String key : getCombineMode().keySet()) {
            if ((sqlMode & getSupportedSqlMode().get(key)) != 0) {
                sqlMode |= getCombineMode().get(key);
            }
        }
        return sqlMode;
    }

    // check if this SQL MODE is supported
    public static boolean isSupportedSqlMode(String sqlMode) {
        return sqlMode != null && getSupportedSqlMode().containsKey(sqlMode);
    }

    // encode sqlMode from string to long
    private static long getCodeFromString(String sqlMode) {
        long code = 0L;
        if (isSupportedSqlMode(sqlMode)) {
            if (isCombineMode(sqlMode)) {
                code |= getCombineMode().get(sqlMode);
            }
            code |= getSupportedSqlMode().get(sqlMode);
        }
        return code;
    }

    // check if this SQL MODE is combine mode
    public static boolean isCombineMode(String key) {
        return COMBINE_MODE_SET.containsKey(key);
    }

    public static Map<String, Long> getSupportedSqlMode() {
        return SQL_MODE_SET;
    }

    public static Map<String, Long> getCombineMode() {
        return COMBINE_MODE_SET;
    }

    public static boolean check(long mask, long mode) {
        return (mask & mode) == mode;
    }
}
