// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.jdbc;

import com.starrocks.catalog.JDBCResource;
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

public class JDBCConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(JDBCConnector.class);

    private final Map<String, String> properties;
    private final String catalogName;

    private ConnectorMetadata metadata;

    public JDBCConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
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
                metadata = new JDBCMetadata(properties);
            } catch (StarRocksConnectorException e) {
                LOG.error("Failed to create jdbc metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }
}
