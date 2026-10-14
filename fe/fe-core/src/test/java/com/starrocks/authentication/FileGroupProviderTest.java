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

package com.starrocks.authentication;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.InvalidConfException;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

public class FileGroupProviderTest {
    @Test
    public void testFileGroupProvider() throws DdlException {
        new MockUp<FileGroupProvider>() {
            @Mock
            public InputStream getPath(String groupFileUrl) throws IOException {
                String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath() + "/" + "file_group";
                return new FileInputStream(path);
            }
        };

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String groupName = "file_group_provider";
        Map<String, String> properties = Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file",
                FileGroupProvider.GROUP_FILE_URL, "file_group");

        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        FileGroupProvider fileGroupProvider = (FileGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Set<String> groups = fileGroupProvider.getGroup(new UserIdentity("harbor", "%"), "harbor");
        Assertions.assertTrue(groups.contains("group1"));
        Assertions.assertTrue(groups.contains("group2"));
    }
    /**
     * Test case: an http(s) `group_file_url` whose server accepts the connection and then says nothing
     * Test point: the read is bounded. Without a timeout this hangs forever, and the same read is reached
     *             from the journal replay thread, where it would stop the FE from applying this and every
     *             later metadata operation - so what has to be asserted is that the call *returns*, not
     *             what it returns.
     */
    @Test
    public void testSilentHttpServerFailsInsteadOfHangingForever() throws Exception {
        int connectTimeout = Config.group_provider_http_connect_timeout_ms;
        int readTimeout = Config.group_provider_http_read_timeout_ms;
        Config.group_provider_http_connect_timeout_ms = 500;
        Config.group_provider_http_read_timeout_ms = 500;

        // Accepts, never answers: exactly the case a connect timeout does not catch.
        try (ServerSocket silent = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            String url = "http://" + silent.getInetAddress().getHostAddress() + ":" + silent.getLocalPort() + "/groups";
            FileGroupProvider provider = new FileGroupProvider("silent_http_provider",
                    Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file",
                            FileGroupProvider.GROUP_FILE_URL, url));

            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(30),
                    () -> Assertions.assertThrows(DdlException.class, provider::init,
                            "an unanswered read must surface as a failed statement"),
                    "init() must not wait on the server indefinitely");
        } finally {
            Config.group_provider_http_connect_timeout_ms = connectTimeout;
            Config.group_provider_http_read_timeout_ms = readTimeout;
        }
    }
    /**
     * Test case: setting either http timeout to a non-positive value
     * Test point: URLConnection reads 0 as "no timeout" - the unbounded read these settings exist to
     *             prevent - and rejects a negative value with an unchecked exception that would surface on
     *             the journal replay thread. Both are refused where every configuration change enters,
     *             so neither can reach the setter through fe.conf or ADMIN SET FRONTEND CONFIG.
     */
    @Test
    public void testNonPositiveHttpTimeoutsAreRejected() throws Exception {
        int connectTimeout = Config.group_provider_http_connect_timeout_ms;
        int readTimeout = Config.group_provider_http_read_timeout_ms;
        try {
            // setConfigField is the path both fe.conf loading and ADMIN SET FRONTEND CONFIG go through;
            // the mutable-config registry the latter also consults is only built by Config.init().
            for (String key : new String[] {"group_provider_http_connect_timeout_ms",
                    "group_provider_http_read_timeout_ms"}) {
                Field field = Config.class.getField(key);
                for (String bad : new String[] {"0", "-1"}) {
                    InvalidConfException e = Assertions.assertThrows(InvalidConfException.class,
                            () -> ConfigBase.setConfigField(field, bad),
                            key + " = " + bad + " must be refused");
                    Assertions.assertTrue(e.getMessage().contains("positive"), e.getMessage());
                }
                ConfigBase.setConfigField(field, "1500");
            }
            Assertions.assertEquals(1500, Config.group_provider_http_connect_timeout_ms);
            Assertions.assertEquals(1500, Config.group_provider_http_read_timeout_ms);
        } finally {
            Config.group_provider_http_connect_timeout_ms = connectTimeout;
            Config.group_provider_http_read_timeout_ms = readTimeout;
        }
    }
}
