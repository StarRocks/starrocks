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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/TypeDef.java

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

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * Represents an anonymous type definition, e.g., used in DDL and CASTs.
 */
public class TypeDef implements ParseNode {
    private Type parsedType;
    private boolean isAnalyzed;

    private final NodePosition pos;

    public TypeDef(Type parsedType) {
        this(parsedType, NodePosition.ZERO);
    }

    public TypeDef(Type parsedType, NodePosition pos) {
        this.pos = pos;
        this.parsedType = parsedType;
    }

    public static TypeDef create(PrimitiveType type) {
        return new TypeDef(ScalarType.createType(type));
    }

    public static TypeDef createDecimal(int precision, int scale) {
        return new TypeDef(ScalarType.createDecimalV2Type(precision, scale));
    }

    public static TypeDef createVarchar(int len) {
        return new TypeDef(ScalarType.createVarchar(len));
    }

    public static TypeDef createChar(int len) {
        return new TypeDef(ScalarType.createCharType(len));
    }

    //
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        analyze();
    }

    public void analyze() throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        // Check the max nesting depth before calling the recursive analyze() to avoid
        // a stack overflow.
        if (parsedType.exceedsMaxNestingDepth()) {
            throw new AnalysisException(String.format(
                    "Type exceeds the maximum nesting depth of %s:\n%s",
                    Type.MAX_NESTING_DEPTH, parsedType.toSql()));
        }
        analyze(parsedType);
        isAnalyzed = true;
    }

    private void analyze(Type type) throws AnalysisException {
        if (!type.isSupported()) {
            throw new AnalysisException("Unsupported data type: " + type.toSql());
        }
        if (type.isScalarType()) {
            analyzeScalarType((ScalarType) type);
        } else if (type.isArrayType()) {
            analyzeArrayType((ArrayType) type);
        } else if (type.isStructType()) {
            analyzeStructType((StructType) type);
        } else if (type.isMapType()) {
            analyzeMapType((MapType) type);
        } else {
            throw new AnalysisException("Unsupported data type: " + type.toSql());
        }
    }

    private void analyzeScalarType(ScalarType scalarType)
            throws AnalysisException {
        PrimitiveType type = scalarType.getPrimitiveType();
        switch (type) {
            case CHAR:
            case VARCHAR: {
                String name;
                int maxLen;
                if (type == PrimitiveType.VARCHAR) {
                    name = "Varchar";
                    maxLen = ScalarType.OLAP_MAX_VARCHAR_LENGTH;
                } else {
                    name = "Char";
                    maxLen = ScalarType.MAX_CHAR_LENGTH;
                }
                int len = scalarType.getLength();
                // len is decided by child, when it is -1.

                if (len <= 0) {
                    throw new AnalysisException(name + " size must be > 0: " + len);
                }
                if (scalarType.getLength() > maxLen) {
                    throw new AnalysisException(
                            name + " size must be <= " + maxLen + ": " + len);
                }
                break;
            }
            case VARBINARY: {
                String name = "VARBINARY";
                int maxLen = ScalarType.OLAP_MAX_VARCHAR_LENGTH;
                int len = scalarType.getLength();
                // len is decided by child, when it is -1.
                if (scalarType.getLength() > maxLen) {
                    throw new AnalysisException(
                            name + " size must be <= " + maxLen + ": " + len);
                }
                break;
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                final String name = scalarType.getPrimitiveType().name();
                final int precision = scalarType.decimalPrecision();
                final int scale = scalarType.decimalScale();
                final int max_precision = PrimitiveType.getMaxPrecisionOfDecimal(scalarType.getPrimitiveType());
                final int max_scale = type.isDecimalV2Type() ? Math.min(9, precision) : precision;
                if (precision < 1 || precision > max_precision) {
                    throw new AnalysisException(
                            String.format("Precision of %s must between 1 and %d, precision was set to: %d.",
                                    name, max_precision, precision));
                }
                if (scale < 0 || scale > max_scale) {
                    throw new AnalysisException(
                            String.format("Scale of %s must between 0 and %d,  scale was set to: %d.",
                                    name, max_scale, scale));
                }
                break;
            }
            case INVALID_TYPE:
                throw new AnalysisException("Invalid type.");
            default:
                break;
        }
    }

    private void analyzeArrayType(ArrayType type) throws AnalysisException {
        Type baseType = Type.getInnermostType(type);
        analyze(baseType);
        if (baseType.isHllType() || baseType.isBitmapType() || baseType.isPseudoType() || baseType.isPercentile()) {
            throw new AnalysisException("Invalid data type: " + type.toSql());
        }
    }

    private void analyzeStructType(StructType type) throws AnalysisException {
        List<StructField> structFields = type.getFields();
        for (StructField structField: structFields) {
            analyze(structField.getType());
        }
    }

    private void analyzeMapType(MapType type) throws AnalysisException {
        Type keyType = type.getKeyType();
        if (!keyType.isValidMapKeyType()) {
            throw new AnalysisException("Invalid map.key's type: " + keyType.toSql() +
                    ", which should be base types");
        }
        analyze(keyType);
        Type valueType = type.getValueType();
        analyze(valueType);
    }

    public Type getType() {
        return parsedType;
    }

    public void setType(Type type) {
        this.parsedType = type;
    }

    @Override
    public String toString() {
        return parsedType.toString();
    }

    @Override
    public String toSql() {
        return parsedType.toSql();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
