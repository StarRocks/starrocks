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

package com.starrocks.connector.elasticsearch;

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class ElasticsearchMetadataTest {

    @Test
    public void testGetTable(@Mocked EsRestClient client) {
        ElasticsearchMetadata metadata = new ElasticsearchMetadata(client, new HashMap<>(), "catalog");
        Assert.assertNull(metadata.getTable(new ConnectContext(), "default_db", "not_exist_index"));
        Assert.assertNull(metadata.getTable(new ConnectContext(), "aaaa", "tbl"));
    }
}
