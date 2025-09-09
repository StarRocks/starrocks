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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SlotRef.java

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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TypeDef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CsvCastUtil {
    private static final int MAX_VARCHAR_LENGTH = Config.max_varchar_length;

    private static final StringLiteral DEFAULT_VALUE_WHEN_NULL = new StringLiteral("");

    public static FunctionCallExpr buildCsvFormatExpressionFromFields(List<Field> fields, boolean useBackendOperator) {
        // Ensure there are at least three parameters: delimiters, quotation marks, and at least one field
        if (fields.size() < 3) {
            throw new SemanticException("csv_format requires at least three parameters");
        }

        // The first two parameters should be string literals (delimiters and quotes)
        if (!fields.get(0).getType().isStringType() || !fields.get(1).getType().isStringType()) {
            throw new SemanticException("First two parameters of csv_format must be string literals");
        }
        List<Expr> columnExprList = convertFieldListToExprList(fields);
        if (useBackendOperator) {
            return buildBeCsvFormatExpression(columnExprList);
        } else {
            return buildFeCsvFormatExpression(columnExprList);
        }
    }

    public static FunctionCallExpr buildCsvFormatExpressionFromExprs(List<Expr> columnExprList, boolean useBackendOperator) {
        // Ensure there are at least three parameters: delimiters, quotation marks, and at least one field
        if (columnExprList.size() < 3) {
            throw new SemanticException("csv_format requires at least three parameters");
        }

        // The first two parameters should be string literals (delimiters and quotes)
        if (!columnExprList.get(0).getType().isStringType() || !columnExprList.get(1).getType().isStringType()) {
            throw new SemanticException("First two parameters of csv_format must be string literals");
        }
        if (useBackendOperator) {
            return buildBeCsvFormatExpression(columnExprList);
        } else {
            return buildFeCsvFormatExpression(columnExprList);
        }
    }


    /**
     * FE expression scheme: Use concat_ws and nested functions
     * including quotation escape and enclosing
     * Format: concat_ws(',', concat('"', replace(coalesce(cast(col1), ''), '"', '""'), '"'), ...)
     */
    public static FunctionCallExpr buildFeCsvFormatExpression(List<Expr> columns) {
        List<Expr> concatWsArgs = new ArrayList<>();

        // The first parameter is the separator
        Expr separatorExpr = columns.get(0);
        concatWsArgs.add(separatorExpr);

        // The second parameter is th enclose
        Expr encloseExpr = columns.get(1);
        String enclose = ((StringLiteral) encloseExpr).getStringValue();

        for (int i = 2; i < columns.size(); i++) {
            Expr column = columns.get(i);
            // Build a formatted expression for each column
            Expr formattedColumn = buildCsvFormattedColumn(column, enclose);
            concatWsArgs.add(formattedColumn);
        }

        return new FunctionCallExpr(FunctionSet.CONCAT_WS, concatWsArgs);
    }


    /**
     * BE operator scheme: Use the efficient csv_format function
     * Format: csv_format(separator, quote, col1, col2, col3, ...)
     */
    public static FunctionCallExpr buildBeCsvFormatExpression(List<Expr> columns) {
        List<Expr> csvFormatArgs = new ArrayList<>();
        if (columns.size() < 3) {
            throw new SemanticException("csv_format need at least 3 parameters");
        }

        // Parameter 3-N: Original data column (the BE layer will handle type conversion and escape)
        for (Expr column : columns) {
            Expr processedColumn = preprocessComplexTypes(column);
            csvFormatArgs.add(processedColumn);
        }
        return new FunctionCallExpr(FunctionSet.CSV_FORMAT, csvFormatArgs);
    }

    /**
     * Preprocessed column: Convert complex types to JSON strings
     * @param column
     * @return
     */
    public static Expr preprocessComplexTypes(Expr column) {
        Type columnType = column.getType();

        // Complex types: Convert to JSON string using to_json
        if (columnType.isComplexType()) {
            return new FunctionCallExpr(
                    FunctionSet.TO_JSON,
                    List.of(column)
            );
        }
        if (!columnType.isStringType()) {
            return new CastExpr(
                    Type.VARCHAR,
                    column
            );
        }
        return column;
    }

    private static Expr castToCsvString(Expr child, String enclose) {
        Type childType = child.getType();

        // Handle NULL values
        Expr coalesceExpr;
        if (childType.isStringType()) {
            // String type: COALESCE(column, '')
            coalesceExpr = buildCoalesce(child, DEFAULT_VALUE_WHEN_NULL);
        } else {
            // Non-string types: Convert first and then handle NULL
            CastExpr castExpr = new CastExpr(
                    new TypeDef(ScalarType.createVarcharType(MAX_VARCHAR_LENGTH)),
                    child
            );
            coalesceExpr = buildCoalesce(castExpr, DEFAULT_VALUE_WHEN_NULL);
        }

        // Handle double quote escape: REPLACE(column, '"', '""')
        return buildQuoteEscape(coalesceExpr, enclose);
    }

    public static FunctionCallExpr buildCoalesce(Expr expr, Expr defaultValue) {
        return new FunctionCallExpr(
                FunctionSet.COALESCE,
                Arrays.asList(expr, defaultValue)
        );
    }

    public static FunctionCallExpr buildQuoteEscape(Expr expr, String enclose) {
        // REPLACE(column, '"', '""')
        return new FunctionCallExpr(
                FunctionSet.REPLACE,
                Arrays.asList(
                        expr,
                        new StringLiteral(enclose),
                        new StringLiteral(enclose + enclose)
                )
        );
    }

    public static Expr buildCsvFormattedColumn(Expr column, String enclose) {
        // Complete CSV column formatting: CONCAT('"', escapeQuotes(castToString(column)), '"')
        Expr casted = castToCsvString(column, enclose);

        return new FunctionCallExpr(
                FunctionSet.CONCAT,
                Arrays.asList(
                        new StringLiteral(enclose),
                        casted,
                        new StringLiteral(enclose)
                )
        );
    }

    public static List<Expr> convertFieldListToExprList(List<Field> fields) {
        if (fields.size() < 3) {
            throw new SemanticException("csv_format need 3 parameters at least");
        }
        List<Expr> exprList = new ArrayList<>();
        StringLiteral separatorLiteral = new StringLiteral(fields.get(0).getName());
        exprList.add(separatorLiteral);
        StringLiteral encloseLiteral = new StringLiteral(fields.get(1).getName());
        exprList.add(encloseLiteral);
        for (int i = 2; i < fields.size(); i++) {
            Field field = fields.get(i);
            exprList.add(convertFieldToExpr(field));
        }
        return exprList;
    }

    private static Expr convertFieldToExpr(Field field) {
        // Create a SlotRef expression to represent the field
        SlotRef slotRef =  new SlotRef(
                field.getRelationAlias() != null ? field.getRelationAlias() : null,
                field.getName()
        );
        slotRef.setType(field.getType());
        return slotRef;
    }
}
