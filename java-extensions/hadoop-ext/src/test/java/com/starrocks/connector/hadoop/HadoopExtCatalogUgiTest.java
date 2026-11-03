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

package com.starrocks.connector.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HadoopExtCatalogUgiTest {

    private static final String USER_A = "user_a@EXAMPLE.COM";
    private static final String USER_B = "user_b@EXAMPLE.COM";
    private static final String KEYTAB_A = "/path/to/user_a.keytab";
    private static final String KEYTAB_B = "/path/to/user_b.keytab";

    @BeforeAll
    public static void setUpKerberosNameRules() {
        // Lets createRemoteUser() accept principal names without a krb5.conf.
        KerberosName.setRules("RULE:[1:$1@$0](.*@EXAMPLE\\.COM)s/@.*//\nDEFAULT");
    }

    /**
     * Logs in without a KDC: every "login" yields a fresh remote user, so object identity tells
     * whether a cached UGI was reused.
     */
    private static class FakeKerberosHadoopExt extends HadoopExt {
        final AtomicInteger logins = new AtomicInteger();
        final AtomicInteger relogins = new AtomicInteger();
        volatile boolean securityEnabled = true;
        volatile int failingLogins = 0;

        @Override
        protected boolean isSecurityEnabled() {
            return securityEnabled;
        }

        @Override
        protected UserGroupInformation loginFromKeytab(String principal, String keytab) throws IOException {
            logins.incrementAndGet();
            if (failingLogins > 0) {
                failingLogins--;
                throw new IOException("Login failure for " + principal);
            }
            return UserGroupInformation.createRemoteUser(principal);
        }

        volatile String failingReloginUser = null;

        @Override
        protected void reloginIfNeeded(UserGroupInformation ugi) throws IOException {
            relogins.incrementAndGet();
            if (ugi.getUserName().equals(failingReloginUser)) {
                throw new IOException("KDC unreachable");
            }
        }
    }

    private static Configuration catalogConf(String principal, String keytab) {
        Configuration conf = new Configuration(false);
        if (principal != null) {
            conf.set(HadoopExt.CATALOG_KERBEROS_PRINCIPAL, principal);
        }
        if (keytab != null) {
            conf.set(HadoopExt.CATALOG_KERBEROS_KEYTAB, keytab);
        }
        return conf;
    }

    @Test
    public void missingKeysReturnNull() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        Assertions.assertNull(ext.getHDFSUGI(null));
        Assertions.assertNull(ext.getHDFSUGI(new Configuration(false)));
        Assertions.assertNull(ext.getHDFSUGI(catalogConf(USER_A, null)));
        Assertions.assertNull(ext.getHDFSUGI(catalogConf(null, KEYTAB_A)));

        // The user-facing catalog properties alone must not switch the identity: only the FE decides.
        Configuration userKeysOnly = new Configuration(false);
        userKeysOnly.set("hadoop.security.authentication", "kerberos");
        userKeysOnly.set("hadoop.kerberos.principal", USER_A);
        userKeysOnly.set("hadoop.kerberos.keytab", KEYTAB_A);
        Assertions.assertNull(ext.getHMSUGI(userKeysOnly));
        Assertions.assertEquals(0, ext.logins.get());
    }

    @Test
    public void samePairReturnsCachedUgi() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        UserGroupInformation first = ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A));
        UserGroupInformation second = ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A));
        Assertions.assertSame(first, second);
        Assertions.assertEquals(USER_A, first.getUserName());
        Assertions.assertEquals(1, ext.logins.get());
        Assertions.assertEquals(2, ext.relogins.get());
    }

    @Test
    public void differentPairReturnsDifferentUgi() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        UserGroupInformation a = ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A));
        UserGroupInformation b = ext.getHDFSUGI(catalogConf(USER_B, KEYTAB_B));
        UserGroupInformation aOtherKeytab = ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_B));
        Assertions.assertNotSame(a, b);
        Assertions.assertNotSame(a, aOtherKeytab);
        Assertions.assertEquals(USER_B, b.getUserName());
        Assertions.assertEquals(3, ext.logins.get());
    }

    @Test
    public void hmsAndHdfsShareUgi() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        Configuration conf = catalogConf(USER_A, KEYTAB_A);
        Assertions.assertSame(ext.getHMSUGI(conf), ext.getHDFSUGI(conf));
        Assertions.assertEquals(1, ext.logins.get());
    }

    @Test
    public void securityDisabledThrows() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        ext.securityEnabled = false;
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A)));
        Assertions.assertTrue(e.getMessage().contains("hadoop.security.authentication"), e.getMessage());
        Assertions.assertEquals(0, ext.logins.get());
    }

    @Test
    public void loginFailureIsNotCached() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        ext.failingLogins = 1;
        UncheckedIOException e = Assertions.assertThrows(UncheckedIOException.class,
                () -> ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A)));
        Assertions.assertTrue(e.getMessage().contains(USER_A), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains(KEYTAB_A), e.getMessage());

        UserGroupInformation retried = ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A));
        Assertions.assertEquals(USER_A, retried.getUserName());
        Assertions.assertEquals(2, ext.logins.get());
    }

    @Test
    public void concurrentFirstUseLogsInOnce() throws Exception {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<UserGroupInformation>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A));
                }));
            }
            start.countDown();
            UserGroupInformation first = futures.get(0).get(30, TimeUnit.SECONDS);
            for (Future<UserGroupInformation> f : futures) {
                Assertions.assertSame(first, f.get(30, TimeUnit.SECONDS));
            }
            Assertions.assertEquals(1, ext.logins.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void backgroundReloginRefreshesEveryCachedUgi() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A));
        ext.getHDFSUGI(catalogConf(USER_B, KEYTAB_B));
        Assertions.assertTrue(ext.isCatalogKerberosReloginScheduled());
        int before = ext.relogins.get();
        ext.reloginCatalogKerberosUgis();
        Assertions.assertEquals(before + 2, ext.relogins.get());
    }

    @Test
    public void backgroundReloginSurvivesOneFailure() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        ext.getHDFSUGI(catalogConf(USER_A, KEYTAB_A));
        ext.getHDFSUGI(catalogConf(USER_B, KEYTAB_B));
        ext.failingReloginUser = USER_A;
        int before = ext.relogins.get();
        Assertions.assertDoesNotThrow(ext::reloginCatalogKerberosUgis);
        Assertions.assertEquals(before + 2, ext.relogins.get());
    }

    @Test
    public void noReloginScheduledWithoutCatalogCredentials() {
        FakeKerberosHadoopExt ext = new FakeKerberosHadoopExt();
        ext.getHDFSUGI(new Configuration(false));
        Assertions.assertFalse(ext.isCatalogKerberosReloginScheduled());
    }
}
