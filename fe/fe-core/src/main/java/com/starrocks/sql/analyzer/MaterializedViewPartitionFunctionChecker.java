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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.starrocks.sql.common.TimeUnitUtils.TIME_MAP;

public class MaterializedViewPartitionFunctionChecker {
    @FunctionalInterface
    public interface CheckPartitionFunction {
        boolean check(Expr expr);
    }

    public static final Map<String, CheckPartitionFunction> FN_NAME_TO_PATTERN;
    public static final String SUPPORTED_DATE_REGEX = "%Y.*%m.*%d";
    public static final Pattern SUPPORTED_DATE_PATTERN = Pattern.compile(SUPPORTED_DATE_REGEX);

    static {
        FN_NAME_TO_PATTERN = Maps.newHashMap();
        // can add some other functions
        FN_NAME_TO_PATTERN.put("date_trunc", MaterializedViewPartitionFunctionChecker::checkDateTrunc);
        FN_NAME_TO_PATTERN.put("str2date", MaterializedViewPartitionFunctionChecker::checkStr2date);
    }

    public static boolean checkDateTrunc(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        String fnNameString = fnExpr.getFnName().getFunction();
        if (!fnNameString.equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            return false;
        }

        if (!(fnExpr.getChild(0) instanceof StringLiteral)) {
            return false;
        }
        String fmt = ((StringLiteral) fnExpr.getChild(0)).getValue();
        if (fmt.equalsIgnoreCase("week")) {
            throw new SemanticException("The function date_trunc used by the materialized view for partition" +
                    " does not support week formatting", expr.getPos());
        }

        Expr child1 = fnExpr.getChild(1);
        if (child1 instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) child1;
            PrimitiveType primitiveType = slotRef.getType().getPrimitiveType();
            // must check slotRef type, because function analyze don't check it.
            return primitiveType == PrimitiveType.DATETIME || primitiveType == PrimitiveType.DATE;
        } else if (child1 instanceof FunctionCallExpr) {
            // date_trunc('hour', time_slice(dt, 'minute'))
            FunctionCallExpr funcExpr = (FunctionCallExpr) child1;
            String name = funcExpr.getFnName().getFunction();
            if (name.equalsIgnoreCase(FunctionSet.TIME_SLICE)) {
                return checkTimeSlice(funcExpr, fmt);
            } else if (name.equalsIgnoreCase(FunctionSet.STR2DATE)) {
                return checkStr2date(funcExpr);
            }
        }
        return false;
    }

    private static boolean checkTimeSlice(FunctionCallExpr funcExpr, String fmt) {
        if (funcExpr.getParams().exprs().size() != 4) {
            return false;
        }

        // TODO: support more functions which do not affect the mv's final partition.
        Expr child0 = funcExpr.getChild(0);
        if (!(child0 instanceof SlotRef)) {
            throw new SemanticException("1th child of the function time_slice used by the materialized view " +
                    "only supports slot ref", funcExpr.getPos());
        }
        // Check time_slice's input type valid
        SlotRef slotRef = (SlotRef) child0;
        PrimitiveType primitiveType = slotRef.getType().getPrimitiveType();
        if (primitiveType != PrimitiveType.DATETIME && primitiveType != PrimitiveType.DATE) {
            return false;
        }
        // Check time_slice's input
        String timeSliceFloor = ((StringLiteral) funcExpr.getChild(3)).getValue();
        if (!timeSliceFloor.equalsIgnoreCase("floor")) {
            return false;
        }
        // NOTE: Ensure time_slice's time unit is less than partition's time unit which
        // will not affect the mv's final partition.
        String timeSliceFmt = ((StringLiteral) funcExpr.getChild(2)).getValue();
        return TIME_MAP.containsKey(timeSliceFmt) && TIME_MAP.containsKey(fmt) &&
                TIME_MAP.get(timeSliceFmt) < TIME_MAP.get(fmt);
    }

    public static boolean checkStr2date(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }

        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        String fnNameString = fnExpr.getFnName().getFunction();
        if (!fnNameString.equalsIgnoreCase(FunctionSet.STR2DATE)) {
            return false;
        }

        Expr child0 = fnExpr.getChild(0);
        if (!(child0 instanceof SlotRef)) {
            return false;
        }

        SlotRef slotRef = (SlotRef) child0;
        PrimitiveType primitiveType = slotRef.getType().getPrimitiveType();
        // must check slotRef type, because function analyze don't check it.
        if (primitiveType != PrimitiveType.CHAR && primitiveType != PrimitiveType.VARCHAR) {
            return false;
        }

        StringLiteral format = fnExpr.getChild(1).cast();
        String dateFormat = format.getStringValue();
        Matcher foreignKeyMatcher = SUPPORTED_DATE_PATTERN.matcher(dateFormat);
        return foreignKeyMatcher.find();
    }
}
