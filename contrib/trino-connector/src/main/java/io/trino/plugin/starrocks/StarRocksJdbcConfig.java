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

package io.trino.plugin.starrocks;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;

import org.mariadb.jdbc.Driver;

import static com.google.common.base.Strings.isNullOrEmpty;

public class StarRocksJdbcConfig
        extends BaseJdbcConfig
{
    public static final Pattern CONNECTION_STRING_PTRN = Pattern.compile("(?<scheme>[\\w\\+:%]+)\\s*"
        + "(?://(?<authority>[^/?#]*))?\\s*"
        + "(?:/(?!\\s*/)(?<path>[^?#]*))?"
        + "(?:\\?(?!\\s*\\?)(?<query>[^#]*))?"
        + "(?:\\s*#(?<fragment>.*))?");
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean driverUseInformationSchema = true;

    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("starrocks.auto-reconnect")
    public StarRocksJdbcConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("starrocks.max-reconnects")
    public StarRocksJdbcConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("starrocks.connection-timeout")
    public StarRocksJdbcConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isDriverUseInformationSchema()
    {
        return driverUseInformationSchema;
    }

    @Config("starrocks.jdbc.use-information-schema")
    @ConfigDescription("Value of useInformationSchema StarRocks JDBC driver connection property")
    public StarRocksJdbcConfig setDriverUseInformationSchema(boolean driverUseInformationSchema)
    {
        this.driverUseInformationSchema = driverUseInformationSchema;
        return this;
    }

    @AssertTrue(message = "Invalid JDBC URL for MariaDB connector")
    public boolean isUrlValid()
    {
        Driver driver = new Driver();
        return driver.acceptsURL(transConnectionUrl(getConnectionUrl()));
    }

    @AssertTrue(message = "Database (catalog) must not be specified in JDBC URL for StarRocks connector")
    public boolean isUrlWithoutDatabase()
    {
        Matcher matcher = CONNECTION_STRING_PTRN.matcher(transConnectionUrl(getConnectionUrl()));
        if (!matcher.matches()) {
            return false;
        }
        String path = matcher.group("path") == null ? null : decode(matcher.group("path")).trim();
        return isNullOrEmpty(path);
    }

    private static String decode(String text) {
        if (isNullOrEmpty(text)) {
            return text;
        }
        try {
            return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // Won't happen.
        }
        return "";
    }

    public static String transConnectionUrl(String connectionUrl) {
        // use org.mariadb.jdbc.Driver for mysql because of gpl protocol
        if (connectionUrl.contains("mysql")) {
            connectionUrl = connectionUrl.replace("mysql", "mariadb");
        }
        return connectionUrl;
    }
}
