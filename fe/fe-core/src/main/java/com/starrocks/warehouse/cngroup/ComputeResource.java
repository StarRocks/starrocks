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
// limitations under the License

package com.starrocks.warehouse.cngroup;

import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.ComputeResourcePB;

/**
 * ComputeResource is a resource acquired from warehouse, t can be a warehouse or a cngroup resource of warehouse which is to
 * represent the resource that can be used for compute.
 */
public interface ComputeResource {

    /**
     * Get the id of the ComputeResource
     * @return: the id of the CNResource
     */
    long getWarehouseId();

    /**
     * Get the id of the worker group that this ComputeResource belongs to.
     * @return: the id of the worker group
     */
    long getWorkerGroupId();

    /** Image v3 form. Only the registered implementations can be persisted. */
    default ComputeResourcePB toProto() {
        throw new IllegalStateException("compute resource " + getClass().getName() + " cannot be persisted");
    }

    static ComputeResource fromProto(ComputeResourcePB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        switch (pb.getKind()) {
            case WAREHOUSE:
                return WarehouseComputeResource.fromProto(pb.getWarehouse());
            default:
                throw new IllegalArgumentException("unknown compute resource kind " + pb.getKind());
        }
    }
}
