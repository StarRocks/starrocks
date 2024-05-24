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

package com.starrocks.mysql.ssl;

import com.starrocks.common.Config;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import javax.net.ssl.SSLContext;

public class SSLContextLoaderTest {

    @Test
    public void testAutoUpdate() throws Exception {
        // set to 0 to disable auto update
        Config.ssl_cert_auto_update_interval_s = 0;

        SSLContextLoader.load();

        Assert.assertNull(SSLContextLoader.getSslContext());

        // change the key store config
        Config.ssl_keystore_location = getKeyFilePath("starrocks1.jks");
        Config.ssl_keystore_password = "starrocks1";
        Config.ssl_key_password = "starrocks1";
        Config.ssl_truststore_location = getKeyFilePath("truststore.jks");
        Config.ssl_truststore_password = "starrocks";

        // after 2 seconds, the sslContext is still null, because the auto update is disabled.
        Thread.sleep(2000L);

        // enable auto update
        Config.ssl_cert_auto_update_interval_s = 1;
        SSLContextLoader.updateAutoRefreshInterval(Config.ssl_cert_auto_update_interval_s);

        // after 3 seconds, the sslContext changed to not null
        Thread.sleep(3000L);
        SSLContext firstContext = SSLContextLoader.getSslContext();
        Assert.assertNotNull(firstContext);

        // change the key store config again
        Config.ssl_keystore_location = getKeyFilePath("starrocks2.jks");
        Config.ssl_keystore_password = "starrocks2";
        Config.ssl_key_password = "starrocks2";
        // after 3 seconds, the sslContext change to anther value
        Thread.sleep(3000L);

        Assert.assertNotNull(SSLContextLoader.getSslContext());
        Assert.assertNotSame(firstContext, SSLContextLoader.getSslContext());
    }

    private String getKeyFilePath(String name) {
        URL now = SSLContextLoaderTest.class.getResource("/");
        return now.getPath() + "/ssl/" + name;
    }
}
