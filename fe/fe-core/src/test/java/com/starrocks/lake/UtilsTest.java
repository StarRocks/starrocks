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

package com.starrocks.lake;

import com.starrocks.common.UserException;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Test;

import java.util.List;

public class UtilsTest {

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    StarOSAgentEpack starOSAgentEpack;

    @Test
    public void testChooseBackend() {

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgentEpack;
            }
        };

        new MockUp<LakeTablet>() {
            @Mock
            public long getPrimaryComputeNodeId(long clusterId) throws UserException {
                throw new UserException("Failed to get primary backend");
            }
        };

        new MockUp<StarOSAgentEpack>() {
            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws UserException {
                throw new UserException("No backend or compute node alive.");
            }
        };
    }
}
