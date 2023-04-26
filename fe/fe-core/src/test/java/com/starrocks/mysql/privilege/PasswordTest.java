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

import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.security.LdapSecurity;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class PasswordTest {

    @Mocked
    public LdapSecurity ldapSecurity;

    @Test
    public void test() {
        new Expectations() {
            {
                LdapSecurity.checkPassword("uid=zhangsan,ou=company,dc=example,dc=com", "123");
                result = true;

                LdapSecurity.checkPassword("uid=zhangsan,ou=company,dc=example,dc=com", "456");
                result = false;

                LdapSecurity.checkPasswordByRoot("lisi", "123");
                result = true;

                LdapSecurity.checkPasswordByRoot("lisi", "456");
                result = false;
            }
        };

        Password password = new Password(new byte[0], AuthPlugin.AUTHENTICATION_LDAP_SIMPLE,
                "uid=zhangsan,ou=company,dc=example,dc=com");
        Assert.assertTrue(password.check("zhangsan", "123".getBytes(StandardCharsets.UTF_8), null));
        Assert.assertTrue(password.checkPlain("zhangsan", "123"));
        Assert.assertFalse(password.check("zhangsan", "456".getBytes(StandardCharsets.UTF_8), null));
        Assert.assertFalse(password.checkPlain("zhangsan", "456"));

        password = new Password(new byte[0], AuthPlugin.AUTHENTICATION_LDAP_SIMPLE, null);
        Assert.assertTrue(password.check("lisi", "123".getBytes(StandardCharsets.UTF_8), null));
        Assert.assertTrue(password.checkPlain("lisi", "123"));
        Assert.assertFalse(password.check("lisi", "456".getBytes(StandardCharsets.UTF_8), null));
        Assert.assertFalse(password.checkPlain("lisi", "456"));

        password = new Password("*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0".getBytes(StandardCharsets.UTF_8),
                AuthPlugin.MYSQL_NATIVE_PASSWORD, "");
        String seed = "dJSH\\]mcwKJlLH[bYunm";
        byte[] scramble = MysqlPassword.scramble(seed.getBytes(StandardCharsets.UTF_8), "passwd");
        Assert.assertTrue(password.check("lisi", scramble, seed.getBytes(StandardCharsets.UTF_8)));
        Assert.assertTrue(password.checkPlain("lisi", "passwd"));

        password = new Password("*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0".getBytes(StandardCharsets.UTF_8), null, "");
        Assert.assertTrue(password.check("lisi", scramble, seed.getBytes(StandardCharsets.UTF_8)));
        Assert.assertTrue(password.checkPlain("lisi", "passwd"));
    }
}
