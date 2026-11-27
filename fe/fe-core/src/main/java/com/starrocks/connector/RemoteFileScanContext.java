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

import com.starrocks.catalog.Table;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.concurrent.locks.ReentrantLock;

/*
  When doing concurrent loading remote files, some variables can be shared and reused to save costs.
  And in this context, we maintain fields can be shared and reused.
 */
public class RemoteFileScanContext {
    public RemoteFileScanContext(Table table) {
        this.tableLocation = table.getTableLocation();
        try {
            this.dbName = table.getCatalogDBName();
            this.tableName = table.getCatalogTableName();
        } catch (NotImplementedException e) {
            // a table type without catalog names: its file lists are found by location only
        }
    }

    public RemoteFileScanContext(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public String tableLocation = null;
    // The catalog names of the table the scan is for, or null: they find the file lists of a table whose location is
    // not at hand, as on an FE asked to drop a table it no longer has in its metastore cache
    public String dbName = null;
    public String tableName = null;

    // ---- concurrent initialization -----
    public ReentrantLock lock = new ReentrantLock();
    public int usedCount = 0;

    // ---- hudi related fields -----
    public HoodieTableFileSystemView hudiFsView = null;
    public HoodieTimeline hudiTimeline = null;
    public HoodieInstant hudiLastInstant = null;

    public void close() {
        if (hudiFsView != null) {
            hudiFsView.close();
            hudiFsView = null;
            hudiTimeline = null;
            hudiLastInstant = null;
        }
    }
}
