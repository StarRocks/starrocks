// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.journal.JournalEntity;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

public class ImpersonatePrivInfoTest {
    private static final Logger LOG = LogManager.getLogger(ImpersonatePrivInfoTest.class);
    @Test
    public void testSerialized() throws Exception {
        UserIdentity harry = new UserIdentity("Harry", "%");
        harry.analyze(SystemInfoService.DEFAULT_CLUSTER);
        UserIdentity gregory = new UserIdentity("Gregory", "%");
        gregory.analyze(SystemInfoService.DEFAULT_CLUSTER);

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
}
