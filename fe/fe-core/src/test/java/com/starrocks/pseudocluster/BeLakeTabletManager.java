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


package com.starrocks.pseudocluster;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.thrift.TCreateTabletReq;

public class BeLakeTabletManager {
    PseudoBackend backend;
    // TODO: update the data structure according to the later needs
    Table<Long, Long, TabletMetadataPB> tablets = HashBasedTable.create();

    BeLakeTabletManager(PseudoBackend backend) {
        this.backend = backend;
    }

    void createTablet(TCreateTabletReq req) {
        TabletMetadataPB metadataPB = new TabletMetadataPB();
        metadataPB.id = req.tablet_id;
        metadataPB.version = req.version;
        // ignore other unused fields

        tablets.put(req.tablet_id, req.version, metadataPB);
    }

    TabletMetadataPB getTablet(long tabletId, long version) {
        return tablets.get(tabletId, version);
    }
}
