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

package com.starrocks.type;

import com.starrocks.sql.common.TypeManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TypeCompatibilityMatrixTest {

    // A pair missing from the matrix is not "incompatible" but a NullPointerException in getAssignmentCompatibleType,
    // and a function lookup asks for pairs it only tries: LAG on VARBINARY or TIME failed with "No assignment from
    // VARBINARY(32) to BITMAP" (StarRocks/starrocks#84288). The matrix's own check skips several types, so ask for
    // every pair of scalar types here.
    @Test
    public void testEveryScalarPairHasAnAnswer() {
        List<ScalarType> types = new ArrayList<>();
        for (PrimitiveType type : PrimitiveType.values()) {
            if (type.isDecimalV3Type()) {
                // the plain DECIMAL32..256 types are wildcards without a precision
                types.add(TypeFactory.createDecimalV3Type(type, PrimitiveType.getMaxPrecisionOfDecimal(type), 2));
                continue;
            }
            try {
                types.add(TypeFactory.createType(type));
            } catch (NullPointerException e) {
                // not a scalar type
            }
        }
        List<String> missing = new ArrayList<>();
        for (ScalarType t1 : types) {
            for (ScalarType t2 : types) {
                for (boolean strict : new boolean[] {true, false}) {
                    try {
                        TypeManager.getAssignmentCompatibleType(t1, t2, strict);
                    } catch (NullPointerException e) {
                        missing.add(t1.getPrimitiveType() + "/" + t2.getPrimitiveType());
                        break;
                    }
                }
            }
        }
        Assertions.assertEquals(List.of(), missing);
    }
}
