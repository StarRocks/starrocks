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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.util.Collections;
import java.util.List;

public final class IvmSchemaCompat {

    private IvmSchemaCompat() {
    }

    /**
     * Position-aligned compatibility check between an analyzed query's output fields and a stored
     * MV schema, including hidden IVM columns (__ROW_ID__, __AGG_STATE_*).
     */
    public static void compare(List<Field> derivedFields, MaterializedView mv) {
        List<Column> orderedAll = mv.getOrderedOutputColumns(true);
        if (derivedFields.size() != orderedAll.size()) {
            throw new SemanticException(MaterializedViewExceptions.inactiveReasonForColumnChanged(
                    Collections.singleton("column count " + orderedAll.size() + " vs " + derivedFields.size())));
        }
        for (int i = 0; i < orderedAll.size(); i++) {
            Column existed = orderedAll.get(i);
            Field derivedField = derivedFields.get(i);
            Type derivedNormalized = AnalyzerUtils.transformTableColumnType(derivedField.getType(), false);
            // Validate hidden IVM columns (__ROW_ID__, __AGG_STATE_*) rather than skip them: a re-derived
            // rewriter can drift a hidden column's type, or its identity -- the __AGG_STATE_* name encodes
            // the agg function/args -- while the visible projection and arity hold.
            if (!isColumnCompatible(existed, derivedNormalized)
                    || (existed.isHidden() && !existed.getName().equalsIgnoreCase(derivedField.getName()))) {
                Column derived = new Column(existed.getName(), derivedNormalized, derivedField.isNullable());
                throw new SemanticException(MaterializedViewExceptions.inactiveReasonForColumnNotCompatible(
                        existed.toString(), derived.toString()));
            }
        }
    }

    // matchesType recurses through ARRAY / MAP / STRUCT — catches inner-type drift like
    // STRUCT<a INT> → STRUCT<a BIGINT> (Spark/Trino ALTER on iceberg) that PrimitiveType.equals
    // misses because non-scalars all return INVALID_TYPE. CHAR/VARCHAR widths stay
    // interchangeable to avoid false positives on iceberg `string`.
    // Nullability skipped: analyzer's NULL on rewritten IVM SELECT diverges from stored NOT NULL.
    // Decimal precision: matchesType only buckets by primitive (DECIMAL32/64/128/256), so
    // DECIMAL(10,2) → DECIMAL(18,2) (both DECIMAL64, same scale) would pass — overflow at
    // refresh INSERT instead. Explicit precision check guards against this.
    public static boolean isColumnCompatible(Column existed, Type derivedType) {
        if (!existed.getType().matchesType(derivedType)) {
            return false;
        }
        if (existed.getType().isDecimalOfAnyVersion() && derivedType.isDecimalOfAnyVersion()) {
            ScalarType existedScalar = (ScalarType) existed.getType();
            ScalarType derivedScalar = (ScalarType) derivedType;
            if (existedScalar.getScalarPrecision() != derivedScalar.getScalarPrecision()
                    || existedScalar.getScalarScale() != derivedScalar.getScalarScale()) {
                return false;
            }
        }
        return true;
    }
}
