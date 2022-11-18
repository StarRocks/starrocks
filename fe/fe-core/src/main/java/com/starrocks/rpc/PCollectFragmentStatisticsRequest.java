// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.rpc;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.clearspring.analytics.util.Lists;
import com.starrocks.proto.PUniqueId;

import java.util.List;

@ProtobufClass
public class PCollectFragmentStatisticsRequest extends AttachmentRequest {

    @Protobuf(fieldType = FieldType.OBJECT, order = 1, required = false)
    PUniqueId queryId;

    List<PUniqueId> fragmentInstanceIds;

    public PCollectFragmentStatisticsRequest() {

    }

    public PCollectFragmentStatisticsRequest(PUniqueId queryId, List<PUniqueId> fragmentInstanceIds) {
        this.queryId = queryId;
        this.fragmentInstanceIds = Lists.newArrayList(fragmentInstanceIds);
    }
}
