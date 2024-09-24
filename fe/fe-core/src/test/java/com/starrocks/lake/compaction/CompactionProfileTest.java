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

package com.starrocks.lake.compaction;

import com.starrocks.proto.CompactStat;
import org.junit.Assert;
import org.junit.Test;

class CompactionProfileTest {
    @Test
    public void testBasic() {
        CompactStat stat = new CompactStat();
        stat.subTaskCount = 1;
        stat.readTimeRemote = 2L;
        stat.readBytesRemote = 3L;
        stat.readTimeLocal = 4L;
        stat.readBytesLocal = 5L;
        stat.inQueueTimeSec = 6;

        CompactionProfile profile = new CompactionProfile(stat);

        String s = profile.toString();
        Assert.assertTrue(s.contains("sub_task_count"));
        Assert.assertTrue(s.contains("read_local_sec"));
        Assert.assertTrue(s.contains("read_local_mb"));
        Assert.assertTrue(s.contains("read_remote_sec"));
        Assert.assertTrue(s.contains("read_remote_mb"));
        Assert.assertTrue(s.contains("in_queue_sec"));
    }
}
