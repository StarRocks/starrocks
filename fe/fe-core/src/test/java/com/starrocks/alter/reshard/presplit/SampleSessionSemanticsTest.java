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

import com.starrocks.common.StarRocksException;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class SampleSessionSemanticsTest {

    // ERROR_IF_OVERFLOW is bit 35: a mode that went through an int, or through its names, would lose it.
    private static final long LOAD_SQL_MODE =
            SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL | SqlModeHelper.MODE_ERROR_IF_OVERFLOW;

    /** A user session with every carried variable away from its default. */
    private static SessionVariable loadSession() throws Exception {
        // A bare session: a context would also become this thread's current one and carry these settings into
        // whatever runs next on the thread.
        SessionVariable session = new SessionVariable();
        session.setSqlMode(LOAD_SQL_MODE);
        session.setTimeZone("Asia/Shanghai");
        session.setCboEqBaseType("varchar");
        session.setDecimalOverflowToDouble(true);
        session.setLargeDecimalUnderlyingType("double");
        // No typed setter for these three; SET reaches them by name.
        GlobalStateMgr.getCurrentState().getVariableMgr().applySessionVariable(Map.of(
                SessionVariable.CBO_DECIMAL_CAST_STRING_STRICT, "false",
                SessionVariable.LOWER_UPPER_SUPPORT_UTF8, "true",
                SessionVariable.ORC_USE_COLUMN_NAMES, "true"), session);
        return session;
    }

    @Test
    public void captureCopiesEveryCarriedVariableOfTheLoadSession() throws Exception {
        SampleSessionSemantics semantics = SampleSessionSemantics.capture(loadSession());

        Assertions.assertEquals(Long.valueOf(LOAD_SQL_MODE), semantics.sqlMode());
        Assertions.assertEquals(Map.of(
                SessionVariable.TIME_ZONE, "Asia/Shanghai",
                SessionVariable.CBO_EQ_BASE_TYPE, "varchar",
                SessionVariable.DECIMAL_OVERFLOW_TO_DOUBLE, "true",
                SessionVariable.LARGE_DECIMAL_UNDERLYING_TYPE, "double",
                SessionVariable.CBO_DECIMAL_CAST_STRING_STRICT, "false",
                SessionVariable.LOWER_UPPER_SUPPORT_UTF8, "true",
                SessionVariable.ORC_USE_COLUMN_NAMES, "true"), semantics.variables());
    }

    @Test
    public void captureCopiesValuesRatherThanKeepingTheSession() throws Exception {
        SessionVariable session = loadSession();
        SampleSessionSemantics semantics = SampleSessionSemantics.capture(session);

        session.setTimeZone("UTC");
        session.setSqlMode(SqlModeHelper.MODE_DEFAULT);

        Assertions.assertEquals("Asia/Shanghai", semantics.variables().get(SessionVariable.TIME_ZONE));
        Assertions.assertEquals(Long.valueOf(LOAD_SQL_MODE), semantics.sqlMode());
    }

    @Test
    public void aVariableTheSessionHoldsNoValueForIsLeftOut() {
        // A mocked session holds no time zone, comparison type or large-decimal type.
        SampleSessionSemantics semantics = SampleSessionSemantics.capture(mock(SessionVariable.class));

        Assertions.assertFalse(semantics.variables().containsKey(SessionVariable.TIME_ZONE));
        Assertions.assertFalse(semantics.variables().containsKey(SessionVariable.CBO_EQ_BASE_TYPE));
        Assertions.assertFalse(semantics.variables().containsKey(SessionVariable.LARGE_DECIMAL_UNDERLYING_TYPE));
    }

    @Test
    public void applyingTheCaptureReproducesTheLoadSession() throws Exception {
        SessionVariable load = loadSession();
        SessionVariable sample = new SessionVariable();

        SampleSessionSemantics.capture(load).applyTo(sample);

        // The flags that only turn a NULL into an error stay off: a row the load filters out must not fail the sample.
        Assertions.assertEquals(LOAD_SQL_MODE & ~SampleSessionSemantics.ERROR_RAISING_SQL_MODES, sample.getSqlMode());
        Assertions.assertNotEquals(0L, sample.getSqlMode() & SqlModeHelper.MODE_DOUBLE_LITERAL);
        Assertions.assertEquals(0L, sample.getSqlMode() & SqlModeHelper.MODE_ERROR_IF_OVERFLOW);
        Assertions.assertEquals(SampleSessionSemantics.capture(load).variables(),
                SampleSessionSemantics.capture(sample).variables());
    }

    @Test
    public void noneLeavesTheSessionAlone() throws Exception {
        SessionVariable session = mock(SessionVariable.class);

        SampleSessionSemantics.NONE.applyTo(session);

        verifyNoInteractions(session);
    }

    @Test
    public void aVariableThatCannotBeSetFailsTheApply() {
        SampleSessionSemantics semantics =
                new SampleSessionSemantics(null, Map.of(SessionVariable.LOWER_UPPER_SUPPORT_UTF8, "maybe"));

        StarRocksException failure = Assertions.assertThrows(StarRocksException.class,
                () -> semantics.applyTo(new SessionVariable()));
        Assertions.assertTrue(failure.getMessage().contains(SessionVariable.LOWER_UPPER_SUPPORT_UTF8),
                failure.getMessage());
    }
}
