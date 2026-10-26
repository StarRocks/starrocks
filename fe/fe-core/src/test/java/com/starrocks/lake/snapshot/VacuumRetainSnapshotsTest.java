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

package com.starrocks.lake.snapshot;

import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.RetainSnapshotPB;
import com.starrocks.proto.VacuumRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class VacuumRetainSnapshotsTest {
    @Test
    public void testSnapshotCapturesShadowState() {
        MaterializedIndex index = new MaterializedIndex(10, MaterializedIndex.IndexState.SHADOW);
        MaterializedIndexSnapshotInfo captured = SnapshotInfoHelper.buildMaterializedIndexSnapshotInfo(index);
        index.setState(MaterializedIndex.IndexState.NORMAL);
        MaterializedIndexSnapshotInfo restored = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(captured), MaterializedIndexSnapshotInfo.class);
        Assertions.assertTrue(restored.isShadow);
        Assertions.assertFalse(SnapshotInfoHelper.buildMaterializedIndexSnapshotInfo(index).isShadow);
    }

    @Test
    public void testVacuumRequestWireRoundTrip() throws Exception {
        VacuumRequest request = new VacuumRequest();
        RetainSnapshotPB visible = new RetainSnapshotPB();
        visible.version = 4L;
        visible.tabletIds = List.of(100L, 101L);
        RetainSnapshotPB unpublished = new RetainSnapshotPB();
        unpublished.version = 5L;
        unpublished.tabletIds = List.of();
        request.retainSnapshots = List.of(visible, unpublished);
        request.retainVersions = List.of(4L, 5L);
        var codec = ProtobufProxy.create(VacuumRequest.class);
        VacuumRequest decoded = codec.decode(codec.encode(request));
        Assertions.assertEquals(request.retainVersions, decoded.retainVersions);
        Assertions.assertEquals(2, decoded.retainSnapshots.size());
        Assertions.assertEquals(4L, decoded.retainSnapshots.get(0).version);
        Assertions.assertEquals(List.of(100L, 101L), decoded.retainSnapshots.get(0).tabletIds);
        Assertions.assertEquals(5L, decoded.retainSnapshots.get(1).version);
    }
}
