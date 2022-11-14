// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.journal.JournalEntity;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;


public class ImpersonatePrivInfoTest {
    private static final Logger LOG = LogManager.getLogger(ImpersonatePrivInfoTest.class);

    @Test
    public void testSerialized() throws Exception {
        UserIdentity harry = new UserIdentity("Harry", "%");
        harry.analyze();
        UserIdentity gregory = new UserIdentity("Gregory", "%");
        gregory.analyze();

        ImpersonatePrivInfo info = new ImpersonatePrivInfo(harry, gregory);
        // 1.2 dump to file
        File tempFile = File.createTempFile("ImpersonatePrivInfoTest", ".image");
        LOG.info("dump to file {}", tempFile.getAbsolutePath());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        JournalEntity je = new JournalEntity();
        je.setData(info);
        je.setOpCode(OperationType.OP_GRANT_IMPERSONATE);
        je.write(dos);
        dos.close();

        // 1.3 load from file
        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        JournalEntity jeReader = new JournalEntity();
        jeReader.readFields(dis);
        ImpersonatePrivInfo readInfo = (ImpersonatePrivInfo) jeReader.getData();
        Assert.assertEquals(readInfo.getAuthorizedUser(), harry);
        Assert.assertEquals(readInfo.getSecuredUser(), gregory);
    }

    @Test
    public void testPersistRole() throws Exception {
        // SET UP
        UtFrameUtils.setUpForPersistTest();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();

        // 1. prepare for test
        // 1.1 create 3 users + 1 role
        Auth auth = GlobalStateMgr.getCurrentState().getAuth();
        UserIdentity harry = new UserIdentity("Harry", "%");
        harry.analyze();
        UserIdentity gregory = new UserIdentity("Gregory", "%");
        gregory.analyze();
        UserIdentity neville = new UserIdentity("Neville", "%");
        neville.analyze();
        String createUserSql = "CREATE USER '%s' IDENTIFIED BY '12345'";
        String[] userNames = {"Harry", "Gregory", "Neville"};
        for (String userName : userNames) {
            CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils
                    .parseStmtWithNewParser(String.format(createUserSql, userName), connectContext);
            auth.createUser(createUserStmt);
        }
        String auror = "auror";
        auth.createRole((CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE ROLE " + auror, connectContext));

        // 1.2 make initialized checkpoint here for later use
        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        auth.saveAuth(pseudoImage.getDataOutputStream(), -1);
        // 1.3 ignore all irrelevant journals by resetting journal queue
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        LOG.info("========= master image dumped.");

        // 2. master grant impersonate
        // 2.1 grant role auror to harry
        auth.grantRole((GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT " + auror + " TO Harry", connectContext));
        // 2.2 grant impersonate to role auror
        auth.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT Impersonate on Gregory To role " + auror, connectContext));
        // 2.3 verify can impersonate
        Assert.assertTrue(auth.canImpersonate(harry, gregory));
        // 2.4 grant role auror to neville
        auth.grantRole((GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT " + auror + " TO Neville", connectContext));
        // 2.5 verify can impersonate
        Assert.assertTrue(auth.canImpersonate(neville, gregory));
        // 2.6 revoke auror from neville
        auth.revokeRole((RevokeRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE " + auror + " FROM Neville", connectContext));
        // 2.7 verify can't impersonate
        Assert.assertFalse(auth.canImpersonate(neville, gregory));
        LOG.info("========= master finished. start to load follower's image");

        // 3. verify follower replay
        // 3.1 follower load initialized checkpoint
        Auth newAuth = Auth.read(pseudoImage.getDataInputStream());
        LOG.info("========= load follower's image finished. start to replay");
        // 3.2 follower replay: grant role auror to harry
        PrivInfo privInfo = (PrivInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_GRANT_ROLE);
        newAuth.replayGrantRole(privInfo);
        // 3.3 follower replay: grant impersonate to role auror
        ImpersonatePrivInfo impersonatePrivInfo = (ImpersonatePrivInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_GRANT_IMPERSONATE);
        newAuth.replayGrantImpersonate(impersonatePrivInfo);
        // 3.4 follower verify can impersonate
        Assert.assertTrue(newAuth.canImpersonate(harry, gregory));
        // 3.5 follower replay: grant role auror to neville
        privInfo = (PrivInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_GRANT_ROLE);
        newAuth.replayGrantRole(privInfo);
        // 3.6 follower verify can impersonate
        Assert.assertTrue(newAuth.canImpersonate(neville, gregory));
        // 3.7 follower replay: revoke auror from neville
        privInfo = (PrivInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_REVOKE_ROLE);
        newAuth.replayRevokeRole(privInfo);
        // 3.8 follower verify can't impersonate
        Assert.assertFalse(newAuth.canImpersonate(neville, gregory));
        LOG.info("========= finished replay ");


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
        Assert.assertEquals(writeChecksum, readChecksum);
        Assert.assertTrue(newAuth.canImpersonate(harry, gregory));
        Assert.assertFalse(newAuth.canImpersonate(neville, gregory));

        // TEAR DOWN
        UtFrameUtils.tearDownForPersisTest();
    }
}
