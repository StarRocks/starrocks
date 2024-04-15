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

package com.starrocks.authentication;

import com.starrocks.common.Config;
import com.starrocks.common.util.ClassUtil;
import com.starrocks.mysql.privilege.AuthPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class CustomAuthenticationProviderFactory {

    private static final Logger LOG = LogManager.getLogger(CustomAuthenticationProviderFactory.class);
    public static final String PLUGIN_NAME = AuthPlugin.AUTHENTICATION_CUSTOM.name();
    private static final String CUSTOM_CONFIG_FILE_NAME = "custom_authentication.conf";
    private static final Properties CUSTOM_CONFIGS = new Properties();

    private static CustomAuthenticationProviderFactory INSTANCE = null;

    private static AuthenticationProvider customAuthenticationProvider;

    public CustomAuthenticationProviderFactory() throws AuthenticationException {
        readCustomConfigs();
        init();
    }

    public AuthenticationProvider getCustomAuthenticationProvider() {
        return customAuthenticationProvider;
    }

    public Properties getCustomConfigs() {
        return CUSTOM_CONFIGS;
    }

    public static UserAuthenticationInfo getUserAuthenticationInfo() {
        UserAuthenticationInfo authenticationInfo = new UserAuthenticationInfo();
        authenticationInfo.extraInfo.put(PLUGIN_NAME, CUSTOM_CONFIGS);
        return authenticationInfo;
    }

    public static CustomAuthenticationProviderFactory getInstance() throws AuthenticationException {
        if (INSTANCE == null) {
            INSTANCE = new CustomAuthenticationProviderFactory();
        }

        return INSTANCE;
    }

    private static void readCustomConfigs() {
        String confFile = System.getenv("STARROCKS_HOME") + File.separator + Config.custom_config_dir + File.separator +
                CUSTOM_CONFIG_FILE_NAME;
        FileReader reader = null;
        try {
            reader = new FileReader(confFile);
            CUSTOM_CONFIGS.load(reader);
        } catch (Exception e) {
            LOG.warn(confFile + " read error. ", e.getMessage());
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception e) {
                LOG.warn("close " + confFile + " reader error. ", e.getMessage());
            }
        }
    }

    private static void init() throws AuthenticationException {
        String className = Config.authorization_custom_class.trim();
        if (className.isEmpty()) {
            throw new AuthenticationException("authorization_custom_class cannot empty when use custom authentication!");
        }
        try {
            customAuthenticationProvider = ClassUtil.initialize(className, AuthenticationProvider.class);
        } catch (Exception e) {
            LOG.warn("Error when initialize custom Authentication class " + className +
                    ", will try to use other Authentication provider", e.getMessage());
        }
    }

}
