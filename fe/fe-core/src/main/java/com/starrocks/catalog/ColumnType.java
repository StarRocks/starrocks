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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/ColumnType.java

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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class ColumnType {
    private static Boolean[][] schemaChangeMatrix;

    static {
        schemaChangeMatrix = new Boolean[PrimitiveType.values().length][PrimitiveType.values().length];

        for (int i = 0; i < schemaChangeMatrix.length; i++) {
            for (int j = 0; j < schemaChangeMatrix[i].length; j++) {
                schemaChangeMatrix[i][j] = (i > 0 && i == j); // 0 is PrimitiveType.INVALID_TYPE
            }
        }

        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.SMALLINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.DATE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.LARGEINT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.FLOAT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.FLOAT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DOUBLE.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.CHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.JSON.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.JSON.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.TINYINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.SMALLINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.FLOAT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.DATE.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DECIMALV2.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL64.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL128.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.DECIMAL32.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.DECIMAL64.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.DECIMAL128.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL64.ordinal()][PrimitiveType.DECIMAL64.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL64.ordinal()][PrimitiveType.DECIMAL128.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL128.ordinal()][PrimitiveType.DECIMAL128.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DATETIME.ordinal()][PrimitiveType.DATE.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DATE.ordinal()][PrimitiveType.DATETIME.ordinal()] = true;
    }

    static boolean isSchemaChangeAllowedInvolvingDecimalV3(Type lhs, Type rhs) {
        if (!lhs.isScalarType() || !rhs.isScalarType()) {
            return false;
        }

        ScalarType lhsScalarType = (ScalarType) lhs;
        ScalarType rhsScalarType = (ScalarType) rhs;
        // change decimalv3 to other types;
        if (lhs.isDecimalV3()) {
            // decimal{v1, v2} is deprecated, so forbid changing decimalv3 to decimal{v1, v2}
            if (rhs.isDecimalV2()) {
                return false;
            }
            // change decimalv3 to varchar type, the string must be sufficient to hold decimalv3
            if (rhs.isVarchar()) {
                return lhsScalarType.getMysqlResultSetFieldLength() <= rhsScalarType.getLength();
            }
            // decimalv3 to decimalv3 schema change must guarantee invariant:
            // 1. rhs's fraction part is wide enough to hold lhs's, namely lhs.scale <= rhs.scale;
            // 1. rhs's integer part is wide enough to hold lhs's,
            //  namely lhs.precision-lhs.scale <= rhs.precision - rhs.scale.
            if (rhs.isDecimalV3()) {
                int lhsScale = lhsScalarType.getScalarScale();
                int lhsPrecision = lhsScalarType.getScalarPrecision();
                int rhsScale = rhsScalarType.getScalarScale();
                int rhsPrecision = rhsScalarType.getScalarPrecision();
                return (lhsScale <= rhsScale) && ((lhsPrecision - lhsScale) <= (rhsPrecision - rhsScale));
            }
            return false;
        }
        if (rhs.isDecimalV3()) {
            // allow deprecated decimal type to upgrade to decimalv3 via schema change
            if (lhs.isDecimalV2()) {
                return rhsScalarType.getScalarScale() == 9 && rhsScalarType.getScalarPrecision() == 27;
            }
            // varchar to decimalv3 allowed
            if (lhs.isVarchar()) {
                return true;
            }
            return false;
        }
        return false;
    }

    static boolean isSchemaChangeAllowed(Type lhs, Type rhs) {
        if (lhs.isDecimalV3() || rhs.isDecimalV3()) {
            return isSchemaChangeAllowedInvolvingDecimalV3(lhs, rhs);
        }
        return schemaChangeMatrix[lhs.getPrimitiveType().ordinal()][rhs.getPrimitiveType().ordinal()];
    }

    public static void write(DataOutput out, Type type) throws IOException {
        Preconditions.checkArgument(type.isScalarType(), "only support scalar type serialization");
        ScalarType scalarType = (ScalarType) type;
        Text.writeString(out, scalarType.getPrimitiveType().name());
        out.writeInt(scalarType.getScalarScale());
        out.writeInt(scalarType.getScalarPrecision());
        out.writeInt(scalarType.getLength());
        // Actually, varcharLimit need not to write here, write true to back compatible
        out.writeBoolean(true);
    }

    public static Type read(DataInput in) throws IOException {
        String type = Text.readString(in);
        PrimitiveType primitiveType;
        // For compatible with old udf/udaf
        if ("DECIMAL".equals(type)) {
            primitiveType = PrimitiveType.DECIMALV2;
        } else {
            primitiveType = PrimitiveType.valueOf(type);
        }
        int scale = in.readInt();
        int precision = in.readInt();
        int len = in.readInt();
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_22) {
            // Useless, just for back compatible
            in.readBoolean();
        }
        return ScalarType.createType(primitiveType, len, precision, scale);
    }
}

