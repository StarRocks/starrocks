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

package com.starrocks.mysql.privilege;

import com.starrocks.mysql.security.LdapSecurity;
import org.junit.Assert;
import org.junit.Test;


public class LdapSecurityTest {

    public LdapSecurity ldapSecurity;

    @Test
    public void testEscapeJava() {
        String input = "admin)(|(uid=*";
        String escaped = ldapSecurity.escapeLdapValue(input);
        Assert.assertFalse(escaped.contains(")"));
        Assert.assertFalse(escaped.contains("("));
        Assert.assertFalse(escaped.contains("|"));
        Assert.assertFalse(escaped.contains("*"));
        Assert.assertFalse(escaped.contains("."));
    }

    @Test
    public void testEscapeJava_NullInput() {
        String input = null;
        String escaped = ldapSecurity.escapeLdapValue(input);
        Assert.assertNull(escaped);
    }

    @Test
    public void testEscapeJava_EmptyString() {
        String input = "";
        String escaped = ldapSecurity.escapeLdapValue(input);
        Assert.assertEquals("", escaped);
    }

    @Test
    public void testEscapeJava_SpecialCharacters() {
        String input = "cn=admin,dc=example,dc=com";
        String escaped = ldapSecurity.escapeLdapValue(input);
        Assert.assertEquals(input, escaped);
    }
}