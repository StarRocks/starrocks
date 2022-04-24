package com.starrocks.server;

import com.google.common.collect.Lists;
import com.starrocks.analysis.CreateCatalogStmt;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.common.io.Text;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.spi.Catalog;
import com.starrocks.spi.ExternalCatalog;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.analysis.ShowCatalogsStmt.CATALOGS_PROC_NODE_TITLE_NAMES;

public class CatalogManager {
    private final ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<>();
    private final CatalogProcNode procNode = new CatalogProcNode();

    public void addCatalog(CreateCatalogStmt stmt) {
        // validate configs
        String type = stmt.getCatalogType();
        String name = stmt.getCatalogName();
        Map<String, String> configs = stmt.getProperties();
        if (name.equals("default")) {

        }

        ConnectorManager connectorManager = GlobalStateMgr.getCurrentState().getConnectorManager();
        connectorManager.addConnector(type, name, configs);

        Catalog catalog = new ExternalCatalog(name, configs, type);

        catalogs.put(name, catalog);
        GlobalStateMgr.getCurrentState().getEditLog().logCreateCatalog(catalog);
    }

    public List<List<String>> getCatalogInfo() {
        return procNode.fetchResult().getRows();
    }

    public void replayCreateCatalog(Catalog catalog) {
        catalogs.put(catalog.getName(), catalog);
        ConnectorManager connectorManager = GlobalStateMgr.getCurrentState().getConnectorManager();
        connectorManager.addConnector(catalog.getType(), catalog.getName(), catalog.getProperties());
    }

    public boolean catalogExist(String catalogName) {
        return catalogs.containsKey(catalogName);
    }

    public class CatalogProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(CATALOGS_PROC_NODE_TITLE_NAMES);

            for (Map.Entry<String, Catalog> entry : catalogs.entrySet()) {
                Catalog catalog = entry.getValue();
                result.addRow(Lists.newArrayList(catalog.getName(), catalog.getType(), catalog.getProperties().toString()));
            }
            return result;
        }
    }

    public static CatalogManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CatalogManager.class);
    }
}
