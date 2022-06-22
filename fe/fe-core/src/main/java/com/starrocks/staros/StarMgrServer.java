// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.journal.JournalSystem;
import com.staros.manager.StarManager;
import com.staros.manager.StarManagerServer;
import com.staros.util.GlobalIdGenerator;
import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.service.FrontendOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StarMgrServer {
    private static final Logger LOG = LogManager.getLogger(StarMgrServer.class);

    StarManagerServer starMgrServer;

    public StarMgrServer() {
        starMgrServer = new StarManagerServer();
    }

    // FOR TEST
    public StarMgrServer(StarManagerServer server) {
        starMgrServer = server;
    }

    public StarManager getStarMgr() {
        return starMgrServer.getStarManager();
    }

    public void start(EditLog editLog, CatalogIdGenerator idGenerator) throws IOException {
        String[] starMgrAddr = Config.starmgr_address.split(":");
        if (starMgrAddr.length != 2) {
            LOG.fatal("Config.starmgr_address {} bad format.", Config.starmgr_address);
            System.exit(-1);
        }
        int port = Integer.parseInt(starMgrAddr[1]);

        if (Config.starmgr_s3_bucket.isEmpty()) {
            LOG.fatal("Config.starmgr_s3_bucket is not set.");
            System.exit(-1);
        }

        // necessary starMgr config setting
        com.staros.util.Config.STARMGR_IP = FrontendOptions.getLocalHostAddress();
        com.staros.util.Config.STARMGR_RPC_PORT = port;
        com.staros.util.Config.S3_BUCKET = Config.starmgr_s3_bucket;

        BDBJEJournalWriter journalWriter = new BDBJEJournalWriter(editLog);
        JournalSystem.overrideJournalWriter(journalWriter);

        StarMgrIdGenerator generator = new StarMgrIdGenerator(idGenerator);
        GlobalIdGenerator.overrideIdGenerator(generator);

        starMgrServer.start(com.staros.util.Config.STARMGR_RPC_PORT);
    }

    public void startBackgroundThreads() {
        getStarMgr().start();
    }

    public void stopBackgroundThreads() {
        getStarMgr().stop();
    }

    public void dumpMeta(DataOutputStream out) throws IOException {
        getStarMgr().dumpMeta(out);
    }

    public void loadMeta(DataInputStream in) throws IOException {
        getStarMgr().loadMeta(in);
    }
}
