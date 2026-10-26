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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.expression.VariableExpr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The load session's variables that decide how a sampled expression parses and evaluates, copied by value so the
 * sample sub-query, which runs in a session of its own, evaluates the WHERE clause and the key projections as the
 * load does:
 * <ul>
 *   <li>{@code sql_mode}: how the rendered SQL parses (whether a decimal literal is a DECIMAL or a DOUBLE) and the
 *       other flags that decide a value -- except those that turn an overflowing cast, a division by zero or a
 *       failing function into an error ({@link #ERROR_RAISING_SQL_MODES}). The sampler adds casts of its own (a
 *       generated column's inputs to their column types), and a row the load filters out must not fail the whole
 *       sample; such a row yields NULL there instead;</li>
 *   <li>{@code time_zone}: how Parquet, ORC and Iceberg timestamps decode;</li>
 *   <li>{@code cbo_eq_base_type}: the type a string and an exact number are compared at;</li>
 *   <li>{@code decimal_overflow_to_double}, {@code large_decimal_underlying_type}: the result type of decimal
 *       arithmetic that outgrows its precision;</li>
 *   <li>{@code cbo_decimal_cast_string_strict}: how a decimal constant folds to a string;</li>
 *   <li>{@code lower_upper_support_utf8}: whether {@code lower} and {@code upper} map UTF-8;</li>
 *   <li>{@code orc_use_column_names}: whether a Hive ORC file is read by column name or by position.</li>
 * </ul>
 *
 * <p>Only these are copied, never the whole session: a copied session would bring along the current catalog -- an
 * internal source is rendered without one, so the sample would read another table -- and the resource group, memory
 * limits and profile switches meant for the load. {@code sqlMode} is a bitmask and is copied as one; the others are
 * read and set by name, as {@link VariableMgr} prints and parses them. A variable the session holds no value for is
 * left out, and {@link #NONE}, with no {@code sqlMode} and no variables, leaves the sample session as it is.
 */
record SampleSessionSemantics(Long sqlMode, Map<String, String> variables) {

    /** Carries nothing: the sample session keeps its own settings. */
    static final SampleSessionSemantics NONE = new SampleSessionSemantics(null, Map.of());

    /** The sql_mode flags the sample session leaves off: each only turns a NULL result into an error. */
    static final long ERROR_RAISING_SQL_MODES = SqlModeHelper.MODE_ERROR_IF_OVERFLOW
            | SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION | SqlModeHelper.MODE_ERROR_FOR_DIVISION_BY_ZERO;

    private static final List<String> VARIABLES_BY_NAME = List.of(
            SessionVariable.TIME_ZONE,
            SessionVariable.CBO_EQ_BASE_TYPE,
            SessionVariable.DECIMAL_OVERFLOW_TO_DOUBLE,
            SessionVariable.LARGE_DECIMAL_UNDERLYING_TYPE,
            SessionVariable.CBO_DECIMAL_CAST_STRING_STRICT,
            SessionVariable.LOWER_UPPER_SUPPORT_UTF8,
            SessionVariable.ORC_USE_COLUMN_NAMES);

    SampleSessionSemantics {
        variables = Map.copyOf(Objects.requireNonNull(variables, "variables"));
    }

    /** The carried variables as {@code session} holds them now. */
    static SampleSessionSemantics capture(SessionVariable session) {
        VariableMgr variableMgr = GlobalStateMgr.getCurrentState().getVariableMgr();
        Map<String, String> variables = new HashMap<>();
        for (String name : VARIABLES_BY_NAME) {
            String value = variableMgr.getValue(session, new VariableExpr(name, SetType.SESSION));
            if (value != null) {
                variables.put(name, value);
            }
        }
        return new SampleSessionSemantics(session.getSqlMode(), variables);
    }

    /**
     * Sets the carried variables on {@code session}. Throws when one cannot be set, so a sample never runs in a
     * session that only partly matches the load's.
     */
    void applyTo(SessionVariable session) throws StarRocksException {
        if (sqlMode != null) {
            session.setSqlMode(sqlMode & ~ERROR_RAISING_SQL_MODES);
        }
        if (variables.isEmpty()) {
            return;
        }
        try {
            GlobalStateMgr.getCurrentState().getVariableMgr().applySessionVariable(variables, session);
        } catch (DdlException failure) {
            throw new StarRocksException("cannot set the load's session variables " + variables.keySet()
                    + " on the sample session: " + failure.getMessage(), failure);
        }
    }
}
