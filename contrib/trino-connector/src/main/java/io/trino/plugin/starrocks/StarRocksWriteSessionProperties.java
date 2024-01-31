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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/StarRocksFE.java

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

package io.trino.plugin.starrocks;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.plugin.starrocks.StarRocksWriteConfig.MAX_ALLOWED_QUERY_TIMEOUT;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static java.lang.String.format;

public class StarRocksWriteSessionProperties
        implements SessionPropertiesProvider
{
    public static final String QUERY_TIMEOUT = "query_timeout";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public StarRocksWriteSessionProperties(StarRocksWriteConfig writeConfig)
    {
        properties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(integerProperty(
                        QUERY_TIMEOUT,
                        "The query timeout duration in seconds",
                        writeConfig.getQueryTimeout(),
                        StarRocksWriteSessionProperties::validateQueryTimeout,
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static int getQueryTimeout(ConnectorSession session)
    {
        return session.getProperty(QUERY_TIMEOUT, Integer.class);
    }

    private static void validateQueryTimeout(int queryTimeout)
    {
        if (queryTimeout < 1) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s must be greater than 0: %s", QUERY_TIMEOUT, queryTimeout));
        }
        if (queryTimeout > MAX_ALLOWED_QUERY_TIMEOUT) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s cannot exceed %s: %s", QUERY_TIMEOUT, MAX_ALLOWED_QUERY_TIMEOUT, queryTimeout));
        }
    }
}
