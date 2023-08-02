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


package com.starrocks.connector.jdbc;

import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.util.Map;

import static com.starrocks.connector.CatalogConnectorMetadata.wrapInfoSchema;

public class JDBCConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(JDBCConnector.class);

    private final Map<String, String> properties;
    private final String catalogName;

    private ConnectorMetadata metadata;
    private final InfoSchemaDb infoSchemaDb;

    public JDBCConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        this.infoSchemaDb = new InfoSchemaDb(catalogName);
        validate(JDBCResource.DRIVER_CLASS);
        validate(JDBCResource.URI);
        validate(JDBCResource.USER);
        validate(JDBCResource.PASSWORD);
        validate(JDBCResource.DRIVER_URL);

        // CHECK_SUM used to check the `Dirver` file's integrity in `be`, we only compute it when creating catalog,
        // and put it into properties and then persisted, when `fe` replay create catalog, we can skip it.
        if (this.properties.get(JDBCResource.CHECK_SUM) == null) {
            computeDriverChecksum();
        }
    }

    private void validate(String propertyKey) {
        String value = properties.get(propertyKey);
        if (value == null) {
            throw new IllegalArgumentException("Missing " + propertyKey + " in properties");
        }
    }

    private void computeDriverChecksum() {
        if (FeConstants.runningUnitTest) {
            // skip checking checksun when running ut
            return;
        }
        try {
            URL url = new URL(properties.get(JDBCResource.DRIVER_URL));
            URLConnection urlConnection = url.openConnection();
            InputStream inputStream = urlConnection.getInputStream();

            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            int bytesRead = 0;
            do {
                bytesRead = inputStream.read(buf);
                if (bytesRead < 0) {
                    break;
                }
                digest.update(buf, 0, bytesRead);
            } while (true);

            String checkSum = Hex.encodeHexString(digest.digest());
            properties.put(JDBCResource.CHECK_SUM, checkSum);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot get driver from url: " + properties.get(JDBCResource.DRIVER_URL));
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new JDBCMetadata(properties, catalogName);
            } catch (StarRocksConnectorException e) {
                LOG.error("Failed to create jdbc metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return wrapInfoSchema(metadata, infoSchemaDb);
    }
}
