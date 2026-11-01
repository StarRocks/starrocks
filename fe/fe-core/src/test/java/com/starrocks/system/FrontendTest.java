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


package com.starrocks.system;

import com.starrocks.ha.FrontendNodeType;
import com.starrocks.persist.HbPackage;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FrontendTest {
    
    @Test
    public void testFeUpdate() {
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "name", "testHost", 1110);
        fe.updateHost("modifiedHost");
        Assertions.assertEquals("modifiedHost", fe.getHost());
    }

    @Test
    public void testHbStatusBadNeedSync() {
        FrontendHbResponse hbResponse = new FrontendHbResponse("BAD", "");
        
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "name", "testHost", 1110);
        boolean needSync = fe.handleHbResponse(hbResponse, true);
        Assertions.assertTrue(needSync);
    }

    @Test
    public void testHbResponseReportsLakeIndexPayloadRebuild() {
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "name", "testHost", 1110);
        Assertions.assertFalse(fe.rebuildsLakeIndexPayload());

        fe.handleHbResponse(okHbResponse(true), true);
        Assertions.assertTrue(fe.rebuildsLakeIndexPayload());

        // A failed heartbeat clears it before the FE counts as dead: the FE may come back on an older version.
        fe.handleHbResponse(new FrontendHbResponse("name", "connect timed out"), true);
        Assertions.assertTrue(fe.isAlive());
        Assertions.assertFalse(fe.rebuildsLakeIndexPayload());

        // An FE from before the rebuild does not report it.
        fe.handleHbResponse(okHbResponse(false), true);
        Assertions.assertTrue(fe.isAlive());
        Assertions.assertFalse(fe.rebuildsLakeIndexPayload());
    }

    @Test
    public void testJournaledHbResponseKeepsLakeIndexPayloadRebuild() {
        // The leader journals the heartbeats it handles, so a follower that becomes leader starts from what they said.
        HbPackage hbPackage = new HbPackage();
        hbPackage.addHbResponse(okHbResponse(true));
        HbPackage replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(hbPackage), HbPackage.class);

        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "name", "testHost", 1110);
        fe.handleHbResponse((FrontendHbResponse) replayed.getHbResults().get(0), true);
        Assertions.assertTrue(fe.rebuildsLakeIndexPayload());
    }

    private static FrontendHbResponse okHbResponse(boolean rebuildsLakeIndexPayload) {
        FrontendHbResponse hbResponse = new FrontendHbResponse("name", 9030, 9020, 1L, System.currentTimeMillis(), 1L,
                "version", 0.0f, 1, "");
        hbResponse.setRebuildsLakeIndexPayload(rebuildsLakeIndexPayload);
        return hbResponse;
    }
}
