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

package com.starrocks.journal;

public enum JournalType {
    FE_META("", "fe_meta"),
    STAR_MGR("starmgr_", "star_mgr");

    private final String prefix;
    private final String metricLabel;

    JournalType(String prefix, String metricLabel) {
        this.prefix = prefix;
        this.metricLabel = metricLabel;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getMetricLabel() {
        return metricLabel;
    }

    public static JournalType fromPrefix(String prefix) {
        String normalizedPrefix = prefix == null ? "" : prefix;
        for (JournalType type : values()) {
            if (type.prefix.equals(normalizedPrefix)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown journal prefix: " + prefix);
    }
}
