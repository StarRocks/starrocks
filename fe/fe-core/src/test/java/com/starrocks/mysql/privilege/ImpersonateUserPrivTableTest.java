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


package com.starrocks.mysql.privilege;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.persist.ImpersonatePrivInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;

public class ImpersonateUserPrivTableTest {

    /**
     * test serialized & deserialized
     */
    @Test
    public void testPersist() throws Exception {
        // SET UP
        UtFrameUtils.setUpForPersistTest();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        // 1. prepare for test
        // 1.1 create 2 users
        Auth auth = GlobalStateMgr.getCurrentState().getAuth();
        UserIdentity harry = UserIdentity.createAnalyzedUserIdentWithIp("Harry", "%");
        auth.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser("create user Harry", ctx));
        UserIdentity gregory = UserIdentity.createAnalyzedUserIdentWithIp("Gregory", "%");
        auth.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser("create user Gregory", ctx));
        // 1.2 make initialized checkpoint here for later use
        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        auth.saveAuth(pseudoImage.getDataOutputStream(), -1);
        // 1.3 ignore all irrelevant journals by resetting journal queue
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        // 2. master grant impersonate
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT impersonate on Gregory to Harry", ctx));

        // 3. verify follower replay
        // 3.1 follower load initialized checkpoint
        Auth newAuth = Auth.read(pseudoImage.getDataInputStream());
        ImpersonatePrivInfo info = (ImpersonatePrivInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(
                OperationType.OP_GRANT_IMPERSONATE);
        // 3.2 follower replay
        newAuth.replayGrantImpersonate(info);
        // 3.3 verify the consistency of metadata between the master & the follower
        Assert.assertTrue(auth.canImpersonate(harry, gregory));

        // 4. verify image
        // 4.1 dump image
        pseudoImage = new UtFrameUtils.PseudoImage();
        long writeChecksum = -1;
        auth.saveAuth(pseudoImage.getDataOutputStream(), writeChecksum);
        writeChecksum = auth.writeAsGson(pseudoImage.getDataOutputStream(), writeChecksum);
        // 4.2 load image to a new auth
        DataInputStream dis = pseudoImage.getDataInputStream();
        newAuth = Auth.read(dis);
        long readChecksum = -1;
        readChecksum = newAuth.readAsGson(dis, readChecksum);
        // 4.3 verify the consistency of metadata between the two
        assert (writeChecksum == readChecksum);
        Assert.assertTrue(auth.canImpersonate(harry, gregory));

        // TEAR DOWN
        UtFrameUtils.tearDownForPersisTest();
    }
}
