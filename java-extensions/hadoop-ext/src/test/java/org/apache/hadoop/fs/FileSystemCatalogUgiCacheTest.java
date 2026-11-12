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

package org.apache.hadoop.fs;

import com.starrocks.connector.hadoop.HadoopExt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileSystemCatalogUgiCacheTest {

    private static final URI LOCAL = URI.create("file:///");

    /** Hands out one remote user per catalog principal, the way the real HadoopExt caches keytab logins. */
    private static class PerPrincipalHadoopExt extends HadoopExt {
        private final Map<String, UserGroupInformation> ugis = new ConcurrentHashMap<>();

        @Override
        public UserGroupInformation getHDFSUGI(Configuration conf) {
            String principal = conf.get(CATALOG_KERBEROS_PRINCIPAL);
            return principal == null ? null : ugis.computeIfAbsent(principal, UserGroupInformation::createRemoteUser);
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        HadoopExt.setInstance(new PerPrincipalHadoopExt());
        FileSystem.closeAll();
    }

    @AfterEach
    public void tearDown() throws Exception {
        HadoopExt.setInstance(null);
        FileSystem.closeAll();
    }

    private static Configuration catalogConf(String principal) {
        Configuration conf = new Configuration();
        if (principal != null) {
            conf.set(HadoopExt.CATALOG_KERBEROS_PRINCIPAL, principal);
            conf.set(HadoopExt.CATALOG_KERBEROS_KEYTAB, "/path/to/" + principal + ".keytab");
        }
        return conf;
    }

    @Test
    public void differentCatalogUgiGetsDifferentFileSystem() throws Exception {
        FileSystem a = FileSystem.get(LOCAL, catalogConf("user_a"));
        FileSystem b = FileSystem.get(LOCAL, catalogConf("user_b"));
        Assertions.assertNotSame(a, b);
    }

    @Test
    public void sameCatalogUgiReusesFileSystem() throws Exception {
        FileSystem first = FileSystem.get(LOCAL, catalogConf("user_a"));
        FileSystem second = FileSystem.get(LOCAL, catalogConf("user_a"));
        Assertions.assertSame(first, second);
    }

    @Test
    public void noCatalogUgiKeepsProcessCache() throws Exception {
        FileSystem first = FileSystem.get(LOCAL, catalogConf(null));
        FileSystem second = FileSystem.get(LOCAL, catalogConf(null));
        Assertions.assertSame(first, second);
        Assertions.assertNotSame(first, FileSystem.get(LOCAL, catalogConf("user_a")));
    }
}
