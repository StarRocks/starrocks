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

package com.starrocks.connector;

import com.google.common.collect.Sets;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;

import java.util.Set;

public class ConnectorTableInfo {
    // There is no need to persist relatedMaterializedViews, we can add this info from mv when it load image and
    // replay create mv journal.
    private Set<MvId> relatedMaterializedViews;

    private ConnectorTableInfo(Set<MvId> relatedMaterializedViews) {
        this.relatedMaterializedViews = relatedMaterializedViews;
    }

    public void updateMetaInfo(ConnectorTableInfo tableInfo) {
        if (relatedMaterializedViews == null) {
            relatedMaterializedViews = Sets.newHashSet(tableInfo.relatedMaterializedViews);
        } else {
            relatedMaterializedViews.addAll(tableInfo.relatedMaterializedViews);
        }
    }

    public void removeMetaInfo(ConnectorTableInfo tableInfo) {
        if (relatedMaterializedViews != null && tableInfo.relatedMaterializedViews != null) {
            relatedMaterializedViews.removeAll(tableInfo.relatedMaterializedViews);
        }
    }

    @Override
    public String toString() {
        return "ConnectorTableInfo {" +
                "relatedMaterializedViews=" + relatedMaterializedViews +
                "}";
    }

    public void seTableInfoForConnectorTable(Table table) {
        if (relatedMaterializedViews != null) {
            for (MvId relatedMaterializedView : relatedMaterializedViews) {
                table.addRelatedMaterializedView(relatedMaterializedView);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Set<MvId> relatedMaterializedViews = Sets.newHashSet();

        public Builder setRelatedMaterializedViews(Set<MvId> relatedMaterializedViews) {
            this.relatedMaterializedViews = relatedMaterializedViews;
            return this;
        }

        public ConnectorTableInfo build() {
            return new ConnectorTableInfo(relatedMaterializedViews);
        }
    }
}
