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

package com.starrocks.epack.warehouse.cngroup;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CNGroupResourceTest {
    @Test
    public void testBasic() {
        CNGroupResource resource1 = CNGroupResource.of(1, 2);

        assertThat(resource1.getWarehouseId()).isEqualTo(1);
        assertThat(resource1.getWorkerGroupId()).isEqualTo(2);
        assertThat(resource1.toString()).contains("warehouseId=1, cnGroupId=2");
    }

    @Test
    public void testCompareTo() {
        CNGroupResource resource1 = CNGroupResource.of(1, 2);
        CNGroupResource resource2 = CNGroupResource.of(1, 2);
        assertThat(resource1).isEqualTo(resource2);
    }
}
