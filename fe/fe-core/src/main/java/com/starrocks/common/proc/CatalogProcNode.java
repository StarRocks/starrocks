// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.common.proc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CatalogProcNode implements ProcDirInterface {
    public static final ImmutableList<String> CATALOG_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Catalog").add("Type").add("Comment")
            .build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String catalogName) throws AnalysisException {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return new DbsProcDir(GlobalStateMgr.getCurrentState());
        }
        return new ExternalDbsProcDir(catalogName);
    }

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(CATALOG_PROC_NODE_TITLE_NAMES);
        ConcurrentHashMap<String, Catalog> catalogs = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogs();
        for (Map.Entry<String, Catalog> entry : catalogs.entrySet()) {
            Catalog catalog = entry.getValue();
            if (catalog == null) {
                continue;
            }
            catalog.getProcNodeData(result);
        }
        result.addRow(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "Internal", "Internal Catalog"));
        return result;
    }
}
