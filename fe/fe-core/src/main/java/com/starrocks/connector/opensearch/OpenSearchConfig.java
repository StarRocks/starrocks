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

package com.starrocks.connector.opensearch;

import com.starrocks.connector.config.Config;
import com.starrocks.connector.config.ConnectorConfig;

public class OpenSearchConfig extends ConnectorConfig {

    public static final String KEY_HOSTS = "hosts";
    public static final String KEY_WAN_ONLY = "wan_only";
    public static final String KEY_DOC_VALUE_SCAN = "doc_value_scan";
    public static final String KEY_KEYWORD_SNIFF = "keyword_sniff";

    @Config(key = KEY_HOSTS, desc = "OpenSearch cluster hosts", defaultValue = "", nullable = false)
    private String[] nodes;

    @Config(key = KEY_WAN_ONLY,
            desc = "Only use specified hosts to access OpenSearch cluster",
            defaultValue = "true")
    private boolean enableWanOnly = true;

    @Config(key = KEY_DOC_VALUE_SCAN,
            desc = "Enable docvalues scan optimization",
            defaultValue = "true")
    private boolean enableDocValueScan = true;
    
    @Config(key = KEY_KEYWORD_SNIFF,
            desc = "Enable keyword sniffing for filtering",
            defaultValue = "true")
    private boolean enableKeywordSniff = true;

    public String[] getNodes() {
        return nodes;
    }

    public void setNodes(String[] nodes) {
        this.nodes = nodes;
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
