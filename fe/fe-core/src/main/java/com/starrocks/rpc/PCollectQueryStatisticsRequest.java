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


package com.starrocks.rpc;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.google.common.collect.Lists;
import com.starrocks.proto.PUniqueId;

import java.util.List;

@ProtobufClass
public class PCollectQueryStatisticsRequest extends AttachmentRequest {

    @Protobuf(fieldType = FieldType.OBJECT, order = 1, required = false)
    List<PUniqueId> queryIds;

    public PCollectQueryStatisticsRequest() {
    }

    public PCollectQueryStatisticsRequest(List<PUniqueId> queryIds) {
        this.queryIds = Lists.newArrayList();
        this.queryIds.addAll(queryIds);
    }
}
