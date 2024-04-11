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
package com.starrocks.epack.authorization.ranger;

import com.starrocks.common.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RangerKerberosAuth {
    static Logger LOG = LoggerFactory.getLogger(RangerKerberosAuth.class);

    public static RangerPluginConfig buildKerberosRangerPluginContext(String serviceType, String serviceName) {
        String principal = Config.ranger_spnego_kerberos_principal;
        String keyTab = Config.ranger_spnego_kerberos_keytab;
        String krb5 = Config.ranger_kerberos_krb5_conf;

        if (!principal.isEmpty() && !keyTab.isEmpty()) {
            LOG.info("Interacting with Ranger Admin Server using Kerberos authentication");
            if (krb5 != null && !krb5.isEmpty()) {
                LOG.info("Load system property java.security.krb5.conf with path : " + krb5);
                System.setProperty("java.security.krb5.conf", krb5);
            }

            Configuration hadoopConf = new Configuration();
            hadoopConf.set("hadoop.security.authorization", "true");
            hadoopConf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(hadoopConf);

            try {
                UserGroupInformation.loginUserFromKeytab(principal, keyTab);
            } catch (IOException ioe) {
                LOG.error("Performing kerberos login failed", ioe);
            }
        } else {
            LOG.info("Interacting with Ranger Admin Server using SIMPLE authentication");
        }

        RangerPluginConfig rangerPluginContext = new RangerPluginConfig(serviceType, serviceName, serviceType,
                null, null, null);
        /*
         * Because the jersey version currently used by ranger conflicts with the
         * jersey version used by other packages in starrocks, we temporarily turn
         * off the cookie authentication switch of Kerberos.
         * starrocks access to ranger is a low-frequency operation.
         * */
        rangerPluginContext.setBoolean("ranger.plugin." + serviceType + ".policy.rest.client.cookie.enabled", false);
        return rangerPluginContext;
    }
}
