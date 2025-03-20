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
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class PasswordTest {

    @Mocked
    public LdapSecurity ldapSecurity;

    @Test
    public void test() {
        new Expectations() {
            {
                LdapSecurity.checkPasswordByRoot("starrocks", "123");
                result = true;

                LdapSecurity.checkPasswordByRoot("<starrocks*>", "123");
                result = true;

                LdapSecurity.checkPasswordByRoot("<starrocks*>", "456");
                result = false;
            }
        };

        String user = "starrocks";
        Assert.assertTrue(ldapSecurity.checkPasswordByRoot(user, "123"));
        String userStr = "<starrocks*>";
        Assert.assertTrue(ldapSecurity.checkPasswordByRoot(userStr, "123"));
        Assert.assertFalse(ldapSecurity.checkPasswordByRoot(userStr, "456"));
    }
}
