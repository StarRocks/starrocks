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

package com.starrocks.sql.automv.lifecycle;

import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MVLifecycleManagerTest {

    @BeforeClass
    public static void setUp() throws Exception {
        AutoMVUtil.mockMVChangeLogPersistence();
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterClass
    public static void teardown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private MVLifecycleManager prepareMVLifecycleManager() {
        MVLifecycleManager mgr = new MVLifecycleManager();
        List<MVName> mvNames = IntStream.range(0, 10).mapToObj(i -> MVName.generateFromQuery("foobar" + i))
                .collect(Collectors.toList());
        mvNames.forEach(mgr::commitCradle);
        return mgr;
    }

    @Test
    public void testSaveAndLoad() throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        MVLifecycleManager mgr = prepareMVLifecycleManager();
        LocalDateTime dateTime = LocalDateTime.of(2024, Month.JANUARY, 2, 3, 45, 56);
        Timestamp t = Timestamp.valueOf(dateTime);
        mgr.updateAuditLatestTimestamp(t.getTime());

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        mgr.save(image.getImageWriter());
        SRMetaBlockReader reader = image.getMetaBlockReader();
        MVLifecycleManager mgr2 = new MVLifecycleManager();
        mgr2.load(reader);
        Optional<Long> optTs = mgr2.getAuditLatestTimestamp();
        Assert.assertTrue(optTs.isPresent());
        Assert.assertEquals(optTs.get().longValue(), t.getTime());
        Assert.assertTrue(mgr2.getNameToMVLifecycles().size() > 1);
        Set<String> mvChangeLogSet1 = mgr.getNameToMVLifecycles().values().stream()
                .map(MVLifecycle::getMVChangeLog)
                .map(Object::toString)
                .collect(Collectors.toSet());
        Set<String> mvChangeLogSet2 = mgr2.getNameToMVLifecycles().values().stream()
                .map(MVLifecycle::getMVChangeLog)
                .map(Object::toString)
                .collect(Collectors.toSet());
        Assert.assertEquals(mvChangeLogSet1, mvChangeLogSet2);
    }

    @Test
    public void testDuplicateDigest() {
        MVLifecycleManager mgr = prepareMVLifecycleManager();
        MVName mvName = mgr.getNameToMVLifecycles().keySet().iterator().next();
        Assert.assertTrue(mgr.contains(mvName.getDigest()));
    }

    @Test
    public void testReplayMVChangeLog() {
        MVLifecycleManager mgr = prepareMVLifecycleManager();
        LocalDateTime dateTime = LocalDateTime.of(2024, Month.JANUARY, 2, 3, 45, 56);
        Timestamp t = Timestamp.valueOf(dateTime);
        mgr.updateAuditLatestTimestamp(t.getTime());

        MVLifecycleManager mgr2 = new MVLifecycleManager();
        mgr.getNameToMVLifecycles().values()
                .forEach(mvLifecycle -> mgr2.replayMVChangeLog(mvLifecycle.getMVChangeLog()));
        mgr2.replayMVChangeLog(mgr.getAuditLatestTimestampChangeLog());

        Assert.assertEquals(mgr.getAuditLatestTimestampChangeLog(), mgr2.getAuditLatestTimestampChangeLog());

        Set<String> mvChangeLogSet1 = mgr.getNameToMVLifecycles().values().stream()
                .map(MVLifecycle::getMVChangeLog)
                .map(Object::toString)
                .collect(Collectors.toSet());

        Set<String> mvChangeLogSet2 = mgr2.getNameToMVLifecycles().values().stream()
                .map(MVLifecycle::getMVChangeLog)
                .map(Object::toString)
                .collect(Collectors.toSet());

        Assert.assertEquals(mvChangeLogSet1, mvChangeLogSet2);
    }
}
