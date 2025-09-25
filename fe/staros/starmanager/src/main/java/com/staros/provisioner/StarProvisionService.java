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

package com.staros.provisioner;

import com.staros.exception.StarException;
import com.staros.proto.DeleteResourceRequest;
import com.staros.proto.DeleteResourceResponse;
import com.staros.proto.GetResourceRequest;
import com.staros.proto.GetResourceResponse;
import com.staros.proto.NodeInfo;
import com.staros.proto.ProvisionResourceRequest;
import com.staros.proto.ProvisionResourceResponse;
import com.staros.proto.ResourceProvisionerGrpc.ResourceProvisionerImplBase;
import com.staros.proto.ScaleResourceRequest;
import com.staros.proto.ScaleResourceResponse;
import com.staros.proto.Status;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class StarProvisionService extends ResourceProvisionerImplBase {

    private final StarProvisionServer server;

    public StarProvisionService(StarProvisionServer server) {
        this.server = server;
    }

    @Override
    public void provisionResource(ProvisionResourceRequest request, StreamObserver<ProvisionResourceResponse> responseObserver) {
        ProvisionResourceResponse.Builder builder = ProvisionResourceResponse.newBuilder();
        try {
            List<NodeInfo> info = server.processProvisionResourceRequest(request.getName(), request.getNumOfNodes());
            builder.addAllInfos(info);
        } catch (StarException exception) {
            builder.setStatus(Status.newBuilder()
                    .setCode(exception.getExceptionCode().ordinal())
                    .setMessage(exception.getMessage())
                    .build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteResource(DeleteResourceRequest request, StreamObserver<DeleteResourceResponse> responseObserver) {
        DeleteResourceResponse.Builder builder = DeleteResourceResponse.newBuilder();
        try {
            server.processDeleteResourceRequest(request.getName());
        } catch (StarException exception) {
            builder.setStatus(Status.newBuilder()
                    .setCode(exception.getExceptionCode().ordinal())
                    .setMessage(exception.getMessage())
                    .build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getResource(GetResourceRequest request, StreamObserver<GetResourceResponse> responseObserver) {
        GetResourceResponse.Builder builder = GetResourceResponse.newBuilder();
        try {
            List<NodeInfo> hosts = server.processGetResourceRequest(request.getName());
            builder.addAllInfos(hosts);
        } catch (StarException exception) {
            builder.setStatus(Status.newBuilder()
                    .setCode(exception.getExceptionCode().ordinal())
                    .setMessage(exception.getMessage())
                    .build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void scaleResource(ScaleResourceRequest request, StreamObserver<ScaleResourceResponse> responseObserver) {
        ScaleResourceResponse.Builder builder = ScaleResourceResponse.newBuilder();
        try {
            List<NodeInfo> hosts = server.processScaleResourceRequest(request.getName(), request.getNumOfNodes());
            builder.addAllInfos(hosts);
        } catch (StarException exception) {
            builder.setStatus(Status.newBuilder()
                    .setCode(exception.getExceptionCode().ordinal())
                    .setMessage(exception.getMessage())
                    .build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
