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

package com.starrocks.connector.elasticsearch;

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EsMajorVersionTest {

    @Test
    public void testParseKnownVersions() {
        Assertions.assertTrue(EsMajorVersion.parse("0.90.0").on(EsMajorVersion.V_0_X));
        Assertions.assertTrue(EsMajorVersion.parse("1.7.0").on(EsMajorVersion.V_1_X));
        Assertions.assertTrue(EsMajorVersion.parse("2.4.1").on(EsMajorVersion.V_2_X));
        Assertions.assertTrue(EsMajorVersion.parse("5.6.16").on(EsMajorVersion.V_5_X));
        Assertions.assertTrue(EsMajorVersion.parse("6.8.23").on(EsMajorVersion.V_6_X));
        Assertions.assertTrue(EsMajorVersion.parse("7.17.0").on(EsMajorVersion.V_7_X));
        Assertions.assertTrue(EsMajorVersion.parse("8.12.0").on(EsMajorVersion.V_8_X));
    }

    @Test
    public void testParseEs9xVersion() {
        EsMajorVersion v9 = EsMajorVersion.parse("9.5.2");
        Assertions.assertEquals(9, v9.major);
        Assertions.assertTrue(v9.on(EsMajorVersion.V_9_X));
        Assertions.assertTrue(v9.onOrAfter(EsMajorVersion.V_7_X));
        Assertions.assertTrue(v9.onOrAfter(EsMajorVersion.V_8_X));
        Assertions.assertTrue(v9.after(EsMajorVersion.V_8_X));
        Assertions.assertFalse(v9.before(EsMajorVersion.V_7_X));

        EsMajorVersion v90 = EsMajorVersion.parse("9.0.0");
        Assertions.assertEquals(9, v90.major);
        Assertions.assertTrue(v90.on(EsMajorVersion.V_9_X));
    }

    @Test
    public void testVersionComparisons() {
        Assertions.assertTrue(EsMajorVersion.V_7_X.onOrAfter(EsMajorVersion.V_6_X));
        Assertions.assertTrue(EsMajorVersion.V_8_X.onOrAfter(EsMajorVersion.V_7_X));
        Assertions.assertTrue(EsMajorVersion.V_7_X.before(EsMajorVersion.V_8_X));
        Assertions.assertTrue(EsMajorVersion.V_6_X.notOn(EsMajorVersion.V_7_X));
    }

    @Test
    public void testParseInvalidVersionThrowsException() {
        Assertions.assertThrows(StarRocksConnectorException.class, () -> EsMajorVersion.parse("invalid_version"));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> EsMajorVersion.parse("3.0.0"));
    }
}
