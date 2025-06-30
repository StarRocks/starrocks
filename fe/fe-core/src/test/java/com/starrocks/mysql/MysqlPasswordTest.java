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

package com.starrocks.mysql;

import com.starrocks.common.ErrorReportException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class MysqlPasswordTest {
    @Test
    public void testMakePassword() {
        Assertions.assertEquals("*6C8989366EAF75BB670AD8EA7A7FC1176A95CEF4",
                new String(MysqlPassword.makeScrambledPassword("mypass")));

        Assertions.assertEquals("", new String(MysqlPassword.makeScrambledPassword("")));

        // null
        Assertions.assertEquals("", new String(MysqlPassword.makeScrambledPassword(null)));

        Assertions.assertEquals("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32",
                new String(MysqlPassword.makeScrambledPassword("aBc@321")));

        Assertions.assertEquals(new String(new byte[0]),
                new String(MysqlPassword.getSaltFromPassword(new byte[0])));

    }

    @Test
    public void testCheckPass() {
        // client
        byte[] publicSeed = MysqlPassword.createRandomString();
        byte[] codePass = MysqlPassword.scramble(publicSeed, "mypass");

        Assertions.assertTrue(MysqlPassword.checkScramble(codePass,
                publicSeed,
                MysqlPassword.getSaltFromPassword("*6C8989366EAF75BB670AD8EA7A7FC1176A95CEF4".getBytes(StandardCharsets.UTF_8))));

        Assertions.assertFalse(MysqlPassword.checkScramble(codePass,
                publicSeed,
                MysqlPassword.getSaltFromPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testCheckPassword() {
        Assertions.assertEquals("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32",
                new String(MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32")));

        Assertions.assertEquals("", new String(MysqlPassword.checkPassword(null)));
    }

    @Test
    public void testCheckPasswdFail() {
        assertThrows(ErrorReportException.class, () -> {
            MysqlPassword.checkPassword("*9A6EC1164108A8D3DA3BE3F35A56F6499B6FC32");
            Assertions.fail("No exception throws");
        });
    }

    @Test
    public void testCheckPasswdFail2() {
        assertThrows(ErrorReportException.class, () -> {
            Assertions.assertNotNull(MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32"));
            MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC3H");
            Assertions.fail("No exception throws");
        });
    }

}