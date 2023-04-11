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

import com.starrocks.connector.config.Config;
import com.starrocks.connector.config.ConnectorConfig;

import static com.starrocks.catalog.EsTable.DOC_VALUE_SCAN;
import static com.starrocks.catalog.EsTable.ES_NET_SSL;
import static com.starrocks.catalog.EsTable.HOSTS;
import static com.starrocks.catalog.EsTable.KEYWORD_SNIFF;
import static com.starrocks.catalog.EsTable.PASSWORD;
import static com.starrocks.catalog.EsTable.USER;
import static com.starrocks.catalog.EsTable.WAN_ONLY;

public class EsConfig extends ConnectorConfig {

    @Config(key = HOSTS, desc = "user when connecting es cluster", defaultValue = "", nullable = false)
    private String[] nodes;

    @Config(key = USER, desc = "user when connecting es cluster", defaultValue = "")
    private String userName = null;

    @Config(key = PASSWORD, desc = "password when connecting es cluster", defaultValue = "")
    private String password = null;

    @Config(key = ES_NET_SSL,
            desc = " Whether the HTTPS protocol can be used to access your Elasticsearch cluster",
            defaultValue = "false")
    private boolean enableSsl;

    @Config(key = WAN_ONLY,
            desc = "indicates whether StarRocks only uses the addresses specified by hosts to access the " +
                    "Elasticsearch cluster and fetch data",
            defaultValue = "true")
    private boolean enableWanOnly;

    @Config(key = DOC_VALUE_SCAN,
            desc = "Whether to enable docvalues scan optimization for fetching fields more fast",
            defaultValue = "true")
    private boolean enableDocValueScan;
    
    @Config(key = KEYWORD_SNIFF,
            desc = " Whether to enable sniffing keyword for filtering more reasonable",
            defaultValue = "true")
    private boolean enableKeywordSniff;

    public String[] getNodes() {
        return nodes;
    }

    public void setNodes(String[] nodes) {
        this.nodes = nodes;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isEnableSsl() {
        return enableSsl;
    }

    public void setEnableSsl(boolean enableSsl) {
        this.enableSsl = enableSsl;
    }

    public boolean isEnableWanOnly() {
        return enableWanOnly;
    }

    public void setEnableWanOnly(boolean enableWanOnly) {
        this.enableWanOnly = enableWanOnly;
    }

    public boolean isEnableDocValueScan() {
        return enableDocValueScan;
    }

    public void setEnableDocValueScan(boolean enableDocValueScan) {
        this.enableDocValueScan = enableDocValueScan;
    }

    public boolean isEnableKeywordSniff() {
        return enableKeywordSniff;
    }

    public void setEnableKeywordSniff(boolean enableKeywordSniff) {
        this.enableKeywordSniff = enableKeywordSniff;
    }
}
