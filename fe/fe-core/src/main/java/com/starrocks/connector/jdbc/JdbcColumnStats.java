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

package com.starrocks.connector.jdbc;

import java.util.OptionalDouble;
import java.util.OptionalInt;

/**
 * Dialect-neutral column statistics handed back by a {@link JDBCSchemaResolver}.
 *
 * <p>The model is deliberately expressed in <em>semantic</em> terms, not in the shape of any one
 * source's catalog: PostgreSQL reads {@code pg_stats}, MySQL would read
 * {@code INFORMATION_SCHEMA}, SQL Server needs a {@code DBCC SHOW_STATISTICS} stored procedure.
 * Whatever a dialect has to do to fill these fields stays inside that dialect's resolver.
 *
 * <p>Every field is optional and independently unknown. A dialect must never invent a value to
 * fill a slot it could not read — the consumer maps an absent field to
 * {@code ColumnStatistic.unknown()}, which is what the optimizer needs in order to fall back to
 * its own defaults instead of trusting a fabricated number.
 */
public class JdbcColumnStats {

    private final OptionalDouble nullsFraction;
    // Absolute number of distinct values. A dialect that natively reports a *ratio* (PostgreSQL's
    // negative n_distinct) converts it to an absolute count itself; the model has one meaning only.
    private final OptionalDouble distinctValues;
    // Average bytes per row for this column. Note this is a per-row average, NOT a total — the
    // consumer feeds it straight into ColumnStatistic.averageRowSize, which has the same meaning.
    private final OptionalInt averageWidth;

    public JdbcColumnStats(OptionalDouble nullsFraction, OptionalDouble distinctValues,
                           OptionalInt averageWidth) {
        this.nullsFraction = nullsFraction == null ? OptionalDouble.empty() : nullsFraction;
        this.distinctValues = distinctValues == null ? OptionalDouble.empty() : distinctValues;
        this.averageWidth = averageWidth == null ? OptionalInt.empty() : averageWidth;
    }

    public OptionalDouble getNullsFraction() {
        return nullsFraction;
    }

    public OptionalDouble getDistinctValues() {
        return distinctValues;
    }

    public OptionalInt getAverageWidth() {
        return averageWidth;
    }

    @Override
    public String toString() {
        return "JdbcColumnStats{nullsFraction=" + nullsFraction + ", distinctValues=" + distinctValues
                + ", averageWidth=" + averageWidth + "}";
    }
}
