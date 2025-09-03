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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.combinator.AggStateDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TypeDef;

import java.util.Set;

import static com.starrocks.catalog.DefaultExpr.isValidDefaultFunction;

public class ColumnDefAnalyzer {
    private static final Set<String> CHARSET_NAMES;

    static {
        CHARSET_NAMES = Sets.newHashSet();
        CHARSET_NAMES.add("utf8");
        CHARSET_NAMES.add("gbk");
    }

    public static void analyze(ColumnDef columnDef, boolean isOlap) throws AnalysisException {
        final String name = columnDef.getName();
        final TypeDef typeDef = columnDef.getTypeDef();
        final Boolean isPartitionColumn = columnDef.isPartitionColumn();

        columnDef.setCharsetName(columnDef.getCharsetName().toLowerCase());
        final String charsetName = columnDef.getCharsetName();

        final AggregateType aggregateType = columnDef.getAggregateType();
        final AggStateDesc aggStateDesc = columnDef.getAggStateDesc();
        final boolean isKey = columnDef.isKey();
        final boolean isAllowNull = columnDef.isAllowNull();
        final ColumnDef.DefaultValueDef defaultValueDef = columnDef.getDefaultValueDef();
        final String defaultCharset = columnDef.getDefaultCharset();

        if (name == null || typeDef == null) {
            throw new AnalysisException("No column name or column type in column definition");
        }
        FeNameFormat.checkColumnName(name, isPartitionColumn);

        // When string type length is not assigned, it needs to be assigned to 1.
        if (typeDef.getType().isScalarType()) {
            final ScalarType targetType = (ScalarType) typeDef.getType();
            if (targetType.getPrimitiveType().isStringType()) {
                if (targetType.getLength() <= 0) {
                    targetType.setLength(1);
                }
            } else {
                // if character setting is not for varchar type, Display unsupported information to the user
                if (!defaultCharset.equalsIgnoreCase(charsetName)) {
                    throw new AnalysisException(
                            "character setting is only supported for type varchar in column definition");
                }
            }
        }

        if (!CHARSET_NAMES.contains(charsetName)) {
            throw new AnalysisException("Unknown charset name: " + charsetName);
        }

        // be is not supported yet,so Display unsupported information to the user
        if (!charsetName.equals(defaultCharset)) {
            throw new AnalysisException("charset name " + charsetName + " is not supported yet in column definition");
        }

        if (aggregateType == AggregateType.SUM) {
            // For the decimal type we extend to decimal128 to avoid overflow
            typeDef.setType(extendedPrecision(typeDef.getType(), Config.enable_legacy_compatibility_for_replication));
        }

        typeDef.analyze();

        Type type = typeDef.getType();

        if (isKey && isOlap && !type.canDistributedBy()) {
            if (type.isFloatingPointType()) {
                throw new AnalysisException(
                        String.format("Invalid data type of key column '%s': '%s', use decimal instead", name, type));
            } else {
                throw new AnalysisException(String.format("Invalid data type of key column '%s': '%s'", name, type));
            }
        }

        // A column is a key column if and only if isKey is true.
        // aggregateType == null does not mean that this is a key column,
        // because when creating a UNIQUE KEY table, aggregateType is implicit.
        if (aggregateType != null && aggregateType != AggregateType.NONE) {
            if (isKey) {
                throw new AnalysisException(
                        String.format("Cannot specify aggregate function '%s' for key column '%s'", aggregateType,
                                name));
            }
            if (!AggregateTypeAnalyzer.checkCompatibility(aggregateType, type)) {
                throw new AnalysisException(
                        String.format("Invalid aggregate function '%s' for '%s'", aggregateType, name));
            }
            // check agg_state_desc
            if (aggregateType == AggregateType.AGG_STATE_UNION) {
                if (aggStateDesc == null) {
                    throw new AnalysisException(
                            String.format("Invalid aggregate function '%s' for '%s'", aggregateType, name));
                }
                // Ensure agg_state_desc is compatible with type
                AggregateFunction aggFunc = aggStateDesc.getAggregateFunction();
                if (aggFunc == null) {
                    throw new AnalysisException(
                            String.format("Invalid aggregate function '%s' for '%s': aggregate function is not found",
                                    aggregateType, name));
                }
                if (!aggFunc.getIntermediateTypeOrReturnType().isFullyCompatible(type)) {
                    throw new AnalysisException(
                            String.format("Invalid aggregate function '%s' for '%s': return type is not compatible, colunm " +
                                            "type: %s, return type: %s",
                                    aggregateType, name, type, aggFunc.getReturnType()));
                }
            }
        } else if (type.isBitmapType() || type.isHllType() || type.isPercentile()) {
            throw new AnalysisException(String.format("No aggregate function specified for '%s'", name));
        }

        if (aggStateDesc != null) {
            if (defaultValueDef.isSet) {
                throw new AnalysisException(String.format("Invalid default value for '%s'", name));
            }
            // not set default value
        }
        if (type.isHllType()) {
            if (defaultValueDef.isSet) {
                throw new AnalysisException(String.format("Invalid default value for '%s'", name));
            }

            columnDef.setDefaultValueDef(ColumnDef.DefaultValueDef.EMPTY_VALUE);
        }
        if (type.isBitmapType()) {
            if (defaultValueDef.isSet) {
                throw new AnalysisException(String.format("Invalid default value for '%s'", name));
            }
            columnDef.setDefaultValueDef(ColumnDef.DefaultValueDef.EMPTY_VALUE);
        }
        if (aggregateType == AggregateType.REPLACE_IF_NOT_NULL) {
            // If aggregate type is REPLACE_IF_NOT_NULL, we set it nullable.
            // If default value is not set, we set it NULL
            columnDef.setAllowNull(true);
            if (!defaultValueDef.isSet) {
                columnDef.setDefaultValueDef(ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE);
            }
        }

        if (!isAllowNull && defaultValueDef == ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE) {
            throw new AnalysisException(String.format("Invalid default value for '%s'", name));
        }

        if (defaultValueDef.isSet && defaultValueDef.expr != null) {
            try {
                validateDefaultValue(type, defaultValueDef.expr);
            } catch (AnalysisException e) {
                throw new AnalysisException(String.format("Invalid default value for '%s': %s", name, e.getMessage()));
            }
        }
    }

    public static Type extendedPrecision(Type type, boolean legacyCompatible) {
        if (legacyCompatible) {
            return type;
        }

        if (type.isDecimalV3()) {
            return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, ((ScalarType) type).getScalarScale());
        }
        return type;
    }

    public static void validateDefaultValue(Type type, Expr defaultExpr) throws AnalysisException {
        if (defaultExpr instanceof StringLiteral) {
            String defaultValue = ((StringLiteral) defaultExpr).getValue();
            Preconditions.checkNotNull(defaultValue);
            if (type.isComplexType()) {
                throw new AnalysisException(String.format("Default value for complex type '%s' not supported", type));
            }
            ScalarType scalarType = (ScalarType) type;
            // check if default value is valid. if not, some literal constructor will throw AnalysisException
            PrimitiveType primitiveType = scalarType.getPrimitiveType();
            switch (primitiveType) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    IntLiteral intLiteral = new IntLiteral(defaultValue, type);
                    break;
                case LARGEINT:
                    LargeIntLiteral largeIntLiteral = new LargeIntLiteral(defaultValue);
                    break;
                case FLOAT:
                    FloatLiteral floatLiteral = new FloatLiteral(defaultValue);
                    if (floatLiteral.getType().isDouble()) {
                        throw new AnalysisException("Default value will loose precision: " + defaultValue);
                    }
                case DOUBLE:
                    FloatLiteral doubleLiteral = new FloatLiteral(defaultValue);
                    break;
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    DecimalLiteral decimalLiteral = new DecimalLiteral(defaultValue);
                    decimalLiteral.checkPrecisionAndScale(scalarType,
                            scalarType.getScalarPrecision(), scalarType.getScalarScale());
                    break;
                case DATE:
                case DATETIME:
                    DateLiteral dateLiteral = new DateLiteral(defaultValue, type);
                    break;
                case CHAR:
                case VARCHAR:
                case HLL:
                    if (defaultValue.length() > scalarType.getLength()) {
                        throw new AnalysisException("Default value is too long: " + defaultValue);
                    }
                    break;
                case BITMAP:
                    break;
                case BOOLEAN:
                    BoolLiteral boolLiteral = new BoolLiteral(defaultValue);
                    break;
                default:
                    throw new AnalysisException(String.format("Cannot add default value for type '%s'", type));
            }
        } else if (defaultExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) defaultExpr;
            String functionName = functionCallExpr.getFnName().getFunction();
            boolean supported = isValidDefaultFunction(functionName + "()");

            if (!supported) {
                throw new AnalysisException(
                        String.format("Default expr for function %s is not supported", functionName));
            }

            // default function current_timestamp currently only support DATETIME type.
            if (FunctionSet.NOW.equalsIgnoreCase(functionName) && type.getPrimitiveType() != PrimitiveType.DATETIME) {
                throw new AnalysisException(String.format("Default function now() for type %s is not supported", type));
            }
            // default function uuid currently only support VARCHAR type.
            if (FunctionSet.UUID.equalsIgnoreCase(functionName) && type.getPrimitiveType() != PrimitiveType.VARCHAR) {
                throw new AnalysisException(String.format("Default function uuid() for type %s is not supported", type));
            }
            if (FunctionSet.UUID.equalsIgnoreCase(functionName) && type.getColumnSize() < 36) {
                throw new AnalysisException("Varchar type length must be greater than 36 for uuid function");
            }
            // default function uuid_numeric currently only support LARGE INT type.
            if (FunctionSet.UUID_NUMERIC.equalsIgnoreCase(functionName) &&
                    type.getPrimitiveType() != PrimitiveType.LARGEINT) {
                throw new AnalysisException(String.format("Default function uuid_numeric() for type %s is not supported",
                        type));
            }
        } else if (defaultExpr instanceof NullLiteral) {
            // nothing to check
        } else {
            throw new AnalysisException(String.format("Unsupported expr %s for default value", defaultExpr));
        }
    }
}
