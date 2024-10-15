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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/*
  When doing concurrent loading remote files, some variables can be shared and reused to save costs.
  And in this context, we maintain fields can be shared and reused.
 */
public class RemoteFileScanContext {
    public RemoteFileScanContext(Table table) {
        this.tableLocation = table.getTableLocation();
    }

    public RemoteFileScanContext(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public String tableLocation = null;

    // ---- concurrent initialization -----
    public AtomicBoolean init = new AtomicBoolean(false);
    public ReentrantLock lock = new ReentrantLock();

    // ---- hudi related fields -----
    public HoodieTableFileSystemView hudiFsView = null;
    public HoodieTimeline hudiTimeline = null;
    public HoodieInstant hudiLastInstant = null;
}
