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

package com.starrocks.analysis;

import com.google.common.collect.ImmutableSortedSet;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.ParsingException;

import java.util.Set;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class OdbcScalarFunctionCall implements ParseNode {
    private final Expr function;

    private final NodePosition pos;

    private static final Set<String> ODBC_SCALAR_STRING_FUNCTIONS =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("ascii").add("bit_length").add("char").add("char_length").add("concat")
                    .add("difference").add("hextoraw").add("insert").add("lcase").add("left")
                    .add("length").add("locate").add("lower").add("ltrim").add("octet_length")
                    .add("rawtohex").add("repeat").add("replace").add("right").add("rtrim")
                    .add("soundex").add("space").add("substr").add("substring").add("ucase")
                    .add("upper").build();

    private static final Set<String> ODBC_SCALAR_NUMERIC_FUNCTIONS =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("abs").add("acos").add("asin").add("atan").add("atan2")
                    .add("bitand").add("bitor").add("ceiling").add("cos").add("cot")
                    .add("degrees").add("exp").add("floor").add("log").add("log10")
                    .add("mod").add("pi").add("power").add("radians").add("rand")
                    .add("round").add("roundmagic").add("sign").add("sin").add("sqrt")
                    .add("tan").add("truncate").build();

    private static final Set<String> ODBC_SCALAR_TIMEDATE_FUNCTIONS =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("curdate").add("curtime").add("datediff").add("dayname").add("dayofmonth")
                    .add("dayofweek").add("dayofyear").add("extract").add("hour").add("minute")
                    .add("month").add("monthname").add("now").add("quarter").add("second")
                    .add("week").add("year").add("current_date").add("current_time").add("current_timestamp").build();

    private static final Set<String> ODBC_SCALAR_SYSTEM_FUNCTIONS =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("cursessionid").add("current_user").add("database").add("identity").add("user").build();

    private static final Set<String> ODBC_SCALAR_INFORMATION_FUNCTIONS =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("user").add("database").add("current_user").build();

    public OdbcScalarFunctionCall(Expr function) {
        this.pos = function.getPos();
        this.function = function;
    }

    public Expr mappingFunction() {
        if (!(function instanceof FunctionCallExpr)) {
            throw new ParsingException(PARSER_ERROR_MSG.invalidOdbcFunc(function.toSql()), function.getPos());
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) function;
        String fnName = functionCallExpr.getFnName().getFunction();

        // for information function
        if (ODBC_SCALAR_INFORMATION_FUNCTIONS.contains(fnName)) {
            return new InformationFunction(fnName, function.getPos());
        }

        if (ODBC_SCALAR_STRING_FUNCTIONS.contains(fnName) || ODBC_SCALAR_NUMERIC_FUNCTIONS.contains(fnName) ||
                ODBC_SCALAR_TIMEDATE_FUNCTIONS.contains(fnName) || ODBC_SCALAR_SYSTEM_FUNCTIONS.contains(fnName)) {
            return function;
        }

        throw new ParsingException(PARSER_ERROR_MSG.invalidOdbcFunc(function.toSql()), function.getPos());
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
