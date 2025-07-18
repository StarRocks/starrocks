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

package com.starrocks.persist;

import com.starrocks.alter.AlterJobMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class RenameMaterializedViewLogTest {

    private String fileName = "./RenameMaterializedViewLogTest";

    @AfterEach
    public void tearDownDrop() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal(@Mocked GlobalStateMgr globalStateMgr,
                           @Injectable Database db, @Injectable MaterializedView table) throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));
        String newMvName = "new_mv_name";
        RenameMaterializedViewLog renameMaterializedViewLog =
                    new RenameMaterializedViewLog(1000, 100, newMvName);
        renameMaterializedViewLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        RenameMaterializedViewLog readRenameLog = RenameMaterializedViewLog.read(in);

        Assertions.assertEquals(readRenameLog.getNewMaterializedViewName(), newMvName);
        Assertions.assertEquals(readRenameLog.getId(), 1000);
        Assertions.assertEquals(readRenameLog.getDbId(), 100);
        in.close();
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                result = db;

                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), anyLong);
                result = table;
            }
        };

        new AlterJobMgr(null, null, null)
                    .replayRenameMaterializedView(renameMaterializedViewLog);
    }

}