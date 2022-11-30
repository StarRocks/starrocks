// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.rpc;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.clearspring.analytics.util.Lists;
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
