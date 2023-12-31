/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.starrocks;

import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.exceptions.CJException;
import com.mysql.jdbc.Driver;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.trino.plugin.jdbc.BaseJdbcConfig;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.mysql.cj.conf.ConnectionUrlParser.parseConnectionString;

public class StarRocksJdbcConfig
        extends BaseJdbcConfig
{
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    // Using `useInformationSchema=true` prevents race condition inside MySQL driver's java.sql.DatabaseMetaData.getColumns
    // implementation, which throw SQL exception when a table disappears during listing.
    // Using `useInformationSchema=false` may provide more diagnostic information (see https://github.com/trinodb/trino/issues/1597)
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
    @ConfigDescription("Value of useInformationSchema MySQL JDBC driver connection property")
    public StarRocksJdbcConfig setDriverUseInformationSchema(boolean driverUseInformationSchema)
    {
        this.driverUseInformationSchema = driverUseInformationSchema;
        return this;
    }

    @AssertTrue(message = "Invalid JDBC URL for MySQL connector")
    public boolean isUrlValid()
    {
        try {
            Driver driver = new Driver();
            return driver.acceptsURL(getConnectionUrl());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AssertTrue(message = "Database (catalog) must not be specified in JDBC URL for MySQL connector")
    public boolean isUrlWithoutDatabase()
    {
        try {
            ConnectionUrlParser parser = parseConnectionString(getConnectionUrl());
            return isNullOrEmpty(parser.getPath());
        }
        catch (CJException ignore) {
            return false;
        }
    }
}
