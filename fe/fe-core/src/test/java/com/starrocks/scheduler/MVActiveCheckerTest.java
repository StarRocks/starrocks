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

package com.starrocks.scheduler;

import com.starrocks.catalog.MvId;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class MVActiveCheckerTest {

    @Test
    public void testOnStoppedClearsActiveInfo() throws Exception {
        MVActiveChecker checker = new MVActiveChecker();

        @SuppressWarnings("unchecked")
        Map<MvId, MVActiveChecker.MvActiveInfo> activeInfo =
                (Map<MvId, MVActiveChecker.MvActiveInfo>) FieldUtils.readDeclaredStaticField(
                        MVActiveChecker.class, "MV_ACTIVE_INFO", true);
        activeInfo.clear();
        activeInfo.put(new MvId(1L, 11L), MVActiveChecker.MvActiveInfo.firstFailure());
        activeInfo.put(new MvId(2L, 22L), MVActiveChecker.MvActiveInfo.firstFailure());
        Assertions.assertEquals(2, activeInfo.size(), "precondition: backoff state populated");

        // MV_ACTIVE_INFO holds per-MV activation backoff windows used only by the leader's
        // activation loop. After demotion the next leader (or this FE on re-election) must
        // start fresh rather than honoring backoff windows from a previous leader session.
        MethodUtils.invokeMethod(checker, true, "onStopped");

        Assertions.assertTrue(activeInfo.isEmpty(),
                "MV_ACTIVE_INFO must be cleared when leader-only daemon stops");
    }
}
