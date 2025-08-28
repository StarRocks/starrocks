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

package com.starrocks.qe;

import com.starrocks.common.DdlException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SqlModeHelperTest {

    @Test
    public void testNormal() throws DdlException {
        String sqlMode = "PIPES_AS_CONCAT";
        Assertions.assertEquals(Long.valueOf(2L), SqlModeHelper.encode(sqlMode));

        sqlMode = "";
        Assertions.assertEquals(Long.valueOf(0L), SqlModeHelper.encode(sqlMode));

        sqlMode = "0,1, PIPES_AS_CONCAT";
        Assertions.assertEquals(Long.valueOf(3L), SqlModeHelper.encode(sqlMode));

        long sqlModeValue = 2L;
        Assertions.assertEquals("PIPES_AS_CONCAT", SqlModeHelper.decode(sqlModeValue));

        sqlModeValue = 0L;
        Assertions.assertEquals("", SqlModeHelper.decode(sqlModeValue));
    }

    @Test
    public void testInvalidSqlMode() {
        assertThrows(DdlException.class, () -> {
            String sqlMode = "PIPES_AS_CONCAT, WRONG_MODE";
            SqlModeHelper.encode(sqlMode);
            Assertions.fail("No exception throws");
        });
    }

    @Test
    public void testInvalidDecode() {
        assertThrows(DdlException.class, () -> {
            long sqlMode = SqlModeHelper.MODE_LAST;
            SqlModeHelper.decode(sqlMode);
            Assertions.fail("No exception throws");
        });
    }
}
