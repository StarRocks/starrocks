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

package com.starrocks.common.util;

import com.aliyun.datalake.common.DlfDataToken;
import com.aliyun.datalake.common.DlfMetaToken;
import com.aliyun.datalake.common.impl.Base64Util;
import com.aliyun.datalake.external.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.utils.HadoopUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.aliyun.datalake.core.constant.DataLakeConfig.FS_OSS_ENDPOINT;
import static com.starrocks.StarRocksFE.STARROCKS_HOME_DIR;

public class DlfUtil {
    private static final Logger LOG = LogManager.getLogger(DlfUtil.class);
    private static Configuration conf = null;

    public static String getRamUser() {
        if (ConnectContext.get() != null) {
            String qualifiedUser = ConnectContext.get().getQualifiedUser();
            if (!Strings.isNullOrEmpty(qualifiedUser)) {
                return getRamUser(qualifiedUser);
            } else if (ConnectContext.get().getCurrentUserIdentity() != null) {
                return getRamUser(ConnectContext.get().getCurrentUserIdentity().getUser());
            } else {
                return "";
            }
        }
        return "";
    }

    public static String getRamUser(String user) {
        if (!Strings.isNullOrEmpty(user)) {
            return GlobalStateMgr.getCurrentState().getAuthenticationMgr().getRamUser(user);
        }
        return user;
    }

    public static Configuration readHadoopConf() {
        if (null != conf) {
            return conf;
        }
        Configuration conf = new Configuration();
        String confPath = STARROCKS_HOME_DIR + "/conf/core-site.xml";
        try {
            Path path = Paths.get(confPath);
            if (Files.exists(path)) {
                String xml = new String(Files.readAllBytes(path));
                HadoopUtils.readHadoopXml(xml, conf);
            } else {
                LOG.warn("Cannot find core-site.xml.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DlfUtil.conf = conf;
        return DlfUtil.conf;
    }

    public static String getMetaToken(String ramUser) {
        // fixed currently, maybe can get from config later
        return "/secret/DLF/meta/" + Base64Util.encodeBase64WithoutPadding(ramUser);
    }

    public static Map<String, String> setDataToken(File dataTokenFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        DlfDataToken dataToken = mapper.readValue(dataTokenFile, DlfDataToken.class);
        Map<String, String> properties = new HashMap<>();
        Configuration conf = DlfUtil.readHadoopConf();
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY, dataToken.getAccessKeyId());
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY, dataToken.getAccessKeySecret());
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT, conf.get(FS_OSS_ENDPOINT));
        properties.put(CloudConfigurationConstants.ALIYUN_OSS_STS_TOKEN, dataToken.getSecurityToken());
        return properties;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getFieldValue(Object obj, String fieldName) {
        Field field;
        try {
            field = obj.getClass().getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        field.setAccessible(true);
        try {
            return (T) field.get(obj);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDataTokenPath(String location) {
        // path can be like
        // dlf://clg-461053f863744879b3fbecf0961cc138-1722246085707/database/db-53c9b442bb4e4954a685ee2a4b28275f/table/tbl-9db089197f7d4116baa16a00e80eebf5
        // dls://clg-paimon-64bbd6353899424fb53eca8628f3a17b-1724206775256/database/db-53cf651ed66b4b12a00302dc9facb4d5/table/tbl-3123c2b0edf94e71b4b72f550f1d6b89/
        String[] parts = location.split("/");
        String clgPart;
        if (parts[0].startsWith("dlf")) {
            clgPart = parts[2].split("-")[0] + "-" + parts[2].split("-")[1];
        } else if (parts[0].startsWith("dls")) {
            clgPart = parts[2].split("-")[0] + "-" + parts[2].split("-")[1] + "-" + parts[2].split("-")[2];
        } else {
            return "";
        }
        String dbPart = parts[4];
        String tablePart = parts[6];
        return DlfUtil.getRamUser() + ":" + clgPart + ":" + dbPart + ":" + tablePart;
    }

    public static DlfMetaToken getDlfToken(String path) {
        try {
            File dlfToken = new File(path);
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(dlfToken, DlfMetaToken.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
