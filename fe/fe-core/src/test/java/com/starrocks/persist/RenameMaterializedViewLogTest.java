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

<<<<<<< HEAD

package com.starrocks.persist;

=======
package com.starrocks.persist;

import com.starrocks.alter.AlterJobMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class RenameMaterializedViewLogTest {

    private String fileName = "./RenameMaterializedViewLogTest";

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    @After
    public void tearDownDrop() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
<<<<<<< HEAD
    public void testNormal() throws IOException {
=======
    public void testNormal(@Mocked GlobalStateMgr globalStateMgr,
                           @Injectable Database db, @Injectable MaterializedView table) throws IOException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));
        String newMvName = "new_mv_name";
        RenameMaterializedViewLog renameMaterializedViewLog =
<<<<<<< HEAD
                new RenameMaterializedViewLog(1000, 100, newMvName);
=======
                    new RenameMaterializedViewLog(1000, 100, newMvName);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        renameMaterializedViewLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        RenameMaterializedViewLog readRenameLog = RenameMaterializedViewLog.read(in);

        Assert.assertEquals(readRenameLog.getNewMaterializedViewName(), newMvName);
        Assert.assertEquals(readRenameLog.getId(), 1000);
        Assert.assertEquals(readRenameLog.getDbId(), 100);
        in.close();
<<<<<<< HEAD
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

}