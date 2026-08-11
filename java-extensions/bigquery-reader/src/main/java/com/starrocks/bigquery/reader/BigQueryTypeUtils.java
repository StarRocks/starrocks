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

package com.starrocks.bigquery.reader;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Utility class for extracting values from Apache Arrow FieldVectors as returned
 * by the BigQuery Storage Read API.
 */
public class BigQueryTypeUtils {

    private BigQueryTypeUtils() {}

    /**
     * Extract the value at {@code rowIndex} from an Arrow {@link FieldVector} and return
     * it as a Java object suitable for passing to {@code ConnectorScanner.appendData()}.
     *
     * @return the value as a String (or primitive wrapper), or {@code null} if the row is null.
     */
    public static Object getArrowValue(FieldVector vector, int rowIndex) {
        if (vector.isNull(rowIndex)) {
            return null;
        }
        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(rowIndex);
        }
        if (vector instanceof IntVector) {
            return (long) ((IntVector) vector).get(rowIndex);
        }
        if (vector instanceof SmallIntVector) {
            return (long) ((SmallIntVector) vector).get(rowIndex);
        }
        if (vector instanceof TinyIntVector) {
            return (long) ((TinyIntVector) vector).get(rowIndex);
        }
        if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(rowIndex);
        }
        if (vector instanceof Float4Vector) {
            return (double) ((Float4Vector) vector).get(rowIndex);
        }
        if (vector instanceof DecimalVector) {
            BigDecimal bd = ((DecimalVector) vector).getObject(rowIndex);
            return bd != null ? bd.toPlainString() : null;
        }
        if (vector instanceof BitVector) {
            return ((BitVector) vector).get(rowIndex) != 0;
        }
        if (vector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) vector).get(rowIndex);
            return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
        }
        if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).get(rowIndex);
        }
        if (vector instanceof DateDayVector) {
            // Days since epoch — return as epoch days long; BigQueryColumnValue.stringValue()
            // will format it as a date string.
            return (long) ((DateDayVector) vector).get(rowIndex);
        }
        if (vector instanceof TimeStampMicroVector) {
            // Microseconds since epoch.
            return ((TimeStampMicroVector) vector).get(rowIndex);
        }
        if (vector instanceof TimeStampVector) {
            return ((TimeStampVector) vector).get(rowIndex);
        }
        if (vector instanceof ListVector || vector instanceof StructVector) {
            // Complex types: convert to string representation for now.
            Object obj = vector.getObject(rowIndex);
            return obj != null ? obj.toString() : null;
        }
        // Default: use Arrow's Object representation and convert to string.
        Object obj = vector.getObject(rowIndex);
        return obj != null ? obj.toString() : null;
    }
}
