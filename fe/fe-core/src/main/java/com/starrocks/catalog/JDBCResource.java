// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.proc.BaseProcResult;
import org.apache.commons.codec.binary.Hex;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.util.Map;

/*
 * External JDBC resource for JDBC table query
 * <p>
 * Example:
 * CREATE EXTERNAL RESOURCE "jdbc_pg"
 * PROPERTIES
 * (
 * "type" = "jdbc",
 * "user" = "user",
 * "password" = "password",
 * "jdbc_uri" = "jdbc:postgresql://127.0.0.1:5432/db"
 * "driver" = "driver_name",
 * );
 * <p>
 * DROP RESOURCE "jdbc_pg";
 * */
public class JDBCResource extends Resource {
    public static final String TYPE = "type";
    public static final String NAME = "name";
    public static final String DRIVER_URL = "driver_url";
    public static final String URI = "jdbc_uri";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String CHECK_SUM = "checksum";
    public static final String DRIVER_CLASS = "driver_class";

    // @TODO is this necessary?
    // private static final String JDBC_TYPE = "jdbc_type";

    @SerializedName(value = "configs")
    private Map<String, String> configs;

    public JDBCResource(String name) {
        super(name, ResourceType.JDBC);
    }

    public JDBCResource(String name, Map<String, String> configs) {
        super(name, ResourceType.JDBC);
        this.configs = configs;
    }

    private void checkProperties(String propertyKey) throws DdlException {
        String value = configs.get(propertyKey);
        if (value == null) {
            throw new DdlException("Missing " + propertyKey + " in properties");
        }
    }

    private void computeDriverChecksum() throws DdlException {
        if (FeConstants.runningUnitTest) {
            // skip checking checksun when running ut
            return;
        }
        try {
            URL url = new URL(getProperty(DRIVER_URL));
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
            configs.put(CHECK_SUM, checkSum);
        } catch (Exception e) {
            throw new DdlException("Cannot get driver from url: " + getProperty(DRIVER_URL));
        }
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        for (String key : properties.keySet()) {
            if (!DRIVER_URL.equals(key) && !URI.equals(key) && !USER.equals(key) && !PASSWORD.equals(key)
                    && !TYPE.equals(key) && !NAME.equals(key) && !DRIVER_CLASS.equals(key)) {
                throw new DdlException("Property " + key + " is unknown");
            }
        }
        configs = properties;

        checkProperties(DRIVER_URL);
        checkProperties(DRIVER_CLASS);
        checkProperties(URI);
        checkProperties(USER);
        checkProperties(PASSWORD);

        computeDriverChecksum();
    }

    public String getProperty(String propertyKey) {
        return configs.get(propertyKey);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(PASSWORD)) {
                continue;
            }
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }
}
