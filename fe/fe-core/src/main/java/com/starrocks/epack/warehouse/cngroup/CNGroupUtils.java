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

package com.starrocks.epack.warehouse.cngroup;

import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.LazyComputeResource;

public class CNGroupUtils {
    /**
     * Try to extract CNGroupResource from the given resource object.
     * NOTE: If the input is a LazyComputeResource that is not yet initialized, this method will return null.
     * @param resource the resource object, which can be CNGroupResource or LazyComputeResource wrapping CNGroupResource
     * @return the extracted CNGroupResource, or null if not found
     */
    public static CNGroupResource getAcquiredCNGroupResource(Object resource) {
        if (resource == null) {
            return null;
        }
        if (resource instanceof CNGroupResource) {
            return (CNGroupResource) resource;
        }
        if (resource instanceof LazyComputeResource) {
            LazyComputeResource lazy = (LazyComputeResource) resource;
            if (!lazy.isInitialized()) {
                return null;
            }
            ComputeResource computeResource = lazy.get();
            if (computeResource instanceof CNGroupResource) {
                return (CNGroupResource) computeResource;
            }
        }
        return null;
    }
}
