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

package com.starrocks.connector.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OdpsConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(OdpsConnector.class);
    private final String catalogName;
    private final Odps odps;
    private final OdpsProperties properties;
    private final AliyunCloudCredential aliyunCloudCredential;

    private ConnectorMetadata metadata;

    public OdpsConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = new OdpsProperties(context.getProperties());
        this.odps = initOdps();
        aliyunCloudCredential = new AliyunCloudCredential(properties.get(OdpsProperties.ACCESS_ID),
                properties.get(OdpsProperties.ACCESS_KEY), properties.get(OdpsProperties.ENDPOINT));
    }

    private Odps initOdps() {
        Account account =
                new AliyunAccount(properties.get(OdpsProperties.ACCESS_ID), properties.get(OdpsProperties.ACCESS_KEY));
        Odps odpsIns = new Odps(account);
        odpsIns.setEndpoint(properties.get(OdpsProperties.ENDPOINT));
        odpsIns.setDefaultProject(properties.get(OdpsProperties.PROJECT));
        return odpsIns;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new OdpsMetadata(odps, catalogName, aliyunCloudCredential, properties);
            } catch (StarRocksConnectorException e) {
                LOG.error("Failed to create jdbc metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }
}
