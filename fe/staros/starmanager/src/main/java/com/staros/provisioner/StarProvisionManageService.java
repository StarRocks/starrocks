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
import com.staros.proto.AddResourceNodeRequest;
import com.staros.proto.AddResourceNodeResponse;
import com.staros.proto.Status;
import com.staros.proto.TestResourceManageServiceGrpc;
import io.grpc.stub.StreamObserver;

public class StarProvisionManageService extends TestResourceManageServiceGrpc.TestResourceManageServiceImplBase {
    private final StarProvisionServer server;

    public StarProvisionManageService(StarProvisionServer server) {
        this.server = server;
    }

    @Override
    public void addNode(AddResourceNodeRequest request, StreamObserver<AddResourceNodeResponse> responseObserver) {
        AddResourceNodeResponse.Builder builder = AddResourceNodeResponse.newBuilder();
        try {
            server.processAddNodeRequest(request.getHost());
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
