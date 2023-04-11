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

package com.starrocks.connector.elasticsearch;

import com.starrocks.catalog.EsTable;
import com.starrocks.connector.config.Config;
import com.starrocks.connector.config.ConnectorConfig;

public class EsConfig extends ConnectorConfig {

    @Config(key = EsTable.HOSTS, desc = "user when connecting es cluster", defaultValue = "", nullable = false)
    private String[] nodes;
    @Config(key = "username", desc = "user when connecting es cluster", defaultValue = "", nullable = false)
    private String userName = null;
    @Config(key = "password", desc = "password when connecting es cluster", defaultValue = "", nullable = false)
    private String password = null;
    @Config(key = EsTable.ES_NET_SSL,
            desc = " Whether the HTTPS protocol can be used to access your Elasticsearch cluster",
            defaultValue = "false")
    private boolean enableSsl;
    @Config(key = EsTable.WAN_ONLY,
            desc = "indicates whether StarRocks only uses the addresses specified by hosts to access the " +
                    "Elasticsearch cluster and fetch data",
            defaultValue = "true")
    private boolean enableWanOnly;
    @Config(key = EsTable.DOC_VALUE_SCAN,
            desc = "Whether to enable docvalues scan optimization for fetching fields more fast",
            defaultValue = "true")
    private boolean enableDocValueScan;
    @Config(key = EsTable.KEYWORD_SNIFF,
            desc = " Whether to enable sniffing keyword for filtering more reasonable",
            defaultValue = "true")
    private boolean enableKeywordSniff;
}
