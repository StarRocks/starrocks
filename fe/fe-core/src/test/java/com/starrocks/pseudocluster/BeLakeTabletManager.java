// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.pseudocluster;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.thrift.TCreateTabletReq;

public class BeLakeTabletManager {
    PseudoBackend backend;
    Table<Long, Long, TabletMetadataPB> tablets = HashBasedTable.create();

    BeLakeTabletManager(PseudoBackend backend) {
        this.backend = backend;
    }

    void createTablet(TCreateTabletReq req) {
        if (req.isSetBase_tablet_id()) {
            throw new RuntimeException("Does not support base tablet in UT now");
        }
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
