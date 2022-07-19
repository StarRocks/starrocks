// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mysql.privilege;

import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.persist.ImpersonatePrivInfo;
import com.starrocks.sql.ast.GrantImpersonateStmt;
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
        UtFrameUtils.setUpForPersistTest();

        // create user
        Auth auth = new Auth();
        UserIdentity harry = UserIdentity.createAnalyzedUserIdentWithIp("Harry", "%");
        auth.createUser(new CreateUserStmt(new UserDesc(harry)));
        UserIdentity gregory = UserIdentity.createAnalyzedUserIdentWithIp("Gregory", "%");
        auth.createUser(new CreateUserStmt(new UserDesc(gregory)));

        // make initialized checkpoint here
        UtFrameUtils.ImageHelper imageHelper = new UtFrameUtils.ImageHelper();
        auth.saveAuth(imageHelper.getDataOutputStream(), -1);

        // master grant impersonte
        UtFrameUtils.JournalReplayerHelper.resetFollowerJournalQueue();
        auth.grantImpersonate(new GrantImpersonateStmt(harry, gregory));

        // follower load initialized checkpoint
        // follower replay
        ImpersonatePrivInfo info = (ImpersonatePrivInfo) UtFrameUtils.JournalReplayerHelper.replayNextJournal();
        Auth newAuth = Auth.read(imageHelper.getDataInputStream());
        newAuth.replayGrantImpersonate(info);
        Assert.assertTrue(auth.canImpersonate(harry, gregory));

        // master load new checkpoint
        imageHelper = new UtFrameUtils.ImageHelper();
        long writeChecksum = -1;
        auth.saveAuth(imageHelper.getDataOutputStream(), writeChecksum);
        writeChecksum = auth.writeAsGson(imageHelper.getDataOutputStream(), writeChecksum);
        DataInputStream dis = imageHelper.getDataInputStream();
        newAuth = Auth.read(dis);
        long readChecksum = -1;
        readChecksum = newAuth.readAsGson(dis, readChecksum);
        assert(writeChecksum == readChecksum);
        Assert.assertTrue(auth.canImpersonate(harry, gregory));

        UtFrameUtils.tearDownForPersisTest();
    }
}
