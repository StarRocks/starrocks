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

package com.starrocks.server;

import static com.starrocks.server.GlobalStateMgr.NEXT_ID_INIT_VALUE;

import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.statistics.StatisticsDb;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.UtFrameUtils;
import java.io.IOException;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LocalMetastoreStatisticsDbTest {
    @Mocked private GlobalStateMgr globalStateMgr;

    @Mocked private CatalogRecycleBin recycleBin;

    @Mocked private ColocateTableIndex colocateTableIndex;

    @Mocked private SRMetaBlockReader reader;

    private LocalMetastore localMetastore;

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;
        localMetastore = new LocalMetastore(globalStateMgr, recycleBin, colocateTableIndex);
    }

    /**
     * Helper method to create an existing statistics database for testing
     */
    private Database createExistingStatisticsDb(long dbId) {
        Database db = new Database(dbId, StatsConstants.STATISTICS_DB_NAME);
        return db;
    }

    /**
     * Helper method to verify statistics database properties
     */
    private void verifyStatisticsDbExists(Database db, long expectedId) {
        Assertions.assertNotNull(db, "Statistics database should exist");
        Assertions.assertEquals(expectedId, db.getId(), "Database ID should match");
        Assertions.assertEquals(
                StatsConstants.STATISTICS_DB_NAME, db.getFullName(), "Database name should be _statistics_");
    }

    @Test
    public void testStatisticsDbNotExistsNewCluster() throws IOException, SRMetaBlockException {
        // Test Case 1: Database does not exist (new cluster)
        // Simulate empty checkpoint load using PseudoImage
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        localMetastore.save(image.getImageWriter());

        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        localMetastore.load(reader);
        reader.close();

        // Verify built-in StatisticsDb was created
        Database db = localMetastore.getDb(StatsConstants.STATISTICS_DB_NAME);
        verifyStatisticsDbExists(db, SystemId.STATISTICS_DB_ID);

        // Verify database can be retrieved by ID
        Database dbById = localMetastore.getDb(SystemId.STATISTICS_DB_ID);
        Assertions.assertNotNull(dbById);
        Assertions.assertEquals(StatsConstants.STATISTICS_DB_NAME, dbById.getFullName());

        // Verify database is protected from deletion
        try {
            localMetastore.dropDb(null, StatsConstants.STATISTICS_DB_NAME, false);
            Assertions.fail("Should throw exception when trying to drop statistics database");
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("Cannot drop system database"),
                    "Exception message should indicate system database protection");
        }
    }

    @Test
    public void testStatisticsDbExistsUpgradeScenario() throws IOException, SRMetaBlockException {
        // Test Case 2: Database already exists (upgrade scenario)
        // Create existing database with user ID (>= 10000)
        long existingDbId = NEXT_ID_INIT_VALUE + 1000; // User database ID
        Database existingDb = createExistingStatisticsDb(existingDbId);

        // Manually register existing database to simulate checkpoint load
        localMetastore.getFullNameToDb().put(StatsConstants.STATISTICS_DB_NAME, existingDb);
        localMetastore.getIdToDb().put(existingDbId, existingDb);

        // Save and reload to simulate checkpoint load
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        localMetastore.save(image.getImageWriter());

        // Create new LocalMetastore instance to simulate restart
        LocalMetastore newLocalMetastore = new LocalMetastore(globalStateMgr, recycleBin, colocateTableIndex);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        newLocalMetastore.load(reader);
        reader.close();

        // Verify existing database ID is preserved (not replaced)
        Database db = newLocalMetastore.getDb(StatsConstants.STATISTICS_DB_NAME);
        verifyStatisticsDbExists(db, existingDbId);
        Assertions.assertNotEquals(SystemId.STATISTICS_DB_ID, db.getId(),
                "Existing database ID should be preserved, not replaced with system ID");

        // Verify existing database can still be retrieved
        Database dbById = newLocalMetastore.getDb(existingDbId);
        Assertions.assertNotNull(dbById);
        Assertions.assertEquals(StatsConstants.STATISTICS_DB_NAME, dbById.getFullName());

        // Verify existing database is protected from deletion
        try {
            newLocalMetastore.dropDb(null, StatsConstants.STATISTICS_DB_NAME, false);
            Assertions.fail("Should throw exception when trying to drop statistics database");
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("Cannot drop system database"),
                    "Exception message should indicate system database protection");
        }
    }

    @Test
    public void testStatisticsDbExistsWithSystemId() throws IOException, SRMetaBlockException {
        // Test Case 3: Database exists but with system ID (edge case)
        // This could happen if database was created with system ID in a previous version
        Database existingDb = createExistingStatisticsDb(SystemId.STATISTICS_DB_ID);

        // Manually register existing database
        localMetastore.getFullNameToDb().put(StatsConstants.STATISTICS_DB_NAME, existingDb);
        localMetastore.getIdToDb().put(SystemId.STATISTICS_DB_ID, existingDb);

        // Save and reload to simulate checkpoint load
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        localMetastore.save(image.getImageWriter());

        // Create new LocalMetastore instance to simulate restart
        LocalMetastore newLocalMetastore = new LocalMetastore(globalStateMgr, recycleBin, colocateTableIndex);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        newLocalMetastore.load(reader);
        reader.close();

        // Verify existing database is used (should not create duplicate)
        Database db = newLocalMetastore.getDb(StatsConstants.STATISTICS_DB_NAME);
        verifyStatisticsDbExists(db, SystemId.STATISTICS_DB_ID);

        // Verify only one database exists with this name
        int count = 0;
        for (Database d : newLocalMetastore.getAllDbs()) {
            if (StatsConstants.STATISTICS_DB_NAME.equals(d.getFullName())) {
                count++;
            }
        }
        Assertions.assertEquals(1, count, "Should have exactly one statistics database");
    }

    @Test
    public void testIsStatisticsDbHelperMethods() {
        // Test helper methods
        Assertions.assertTrue(StatisticsDb.isStatisticsDb(StatsConstants.STATISTICS_DB_NAME));
        Assertions.assertTrue(StatisticsDb.isStatisticsDb("_STATISTICS_"));
        Assertions.assertTrue(StatisticsDb.isStatisticsDb("_statistics_"));
        Assertions.assertFalse(StatisticsDb.isStatisticsDb("information_schema"));
        Assertions.assertFalse(StatisticsDb.isStatisticsDb((String) null));

        Assertions.assertTrue(StatisticsDb.isStatisticsDb(SystemId.STATISTICS_DB_ID));
        Assertions.assertFalse(StatisticsDb.isStatisticsDb(SystemId.INFORMATION_SCHEMA_DB_ID));
        Assertions.assertFalse(StatisticsDb.isStatisticsDb((Long) null));
    }
}
