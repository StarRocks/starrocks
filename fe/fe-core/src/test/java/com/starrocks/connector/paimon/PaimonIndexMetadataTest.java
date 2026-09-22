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

package com.starrocks.connector.paimon;

import com.starrocks.connector.index.ConnectorIndexType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PaimonIndexMetadataTest {
    @Test
    public void testPaimonProviderMapping() {
        Assertions.assertEquals(ConnectorIndexType.BITMAP, PaimonMetadata.toConnectorIndexType("bitmap"));
        Assertions.assertEquals(ConnectorIndexType.RANGE, PaimonMetadata.toConnectorIndexType("btree"));
        Assertions.assertEquals(ConnectorIndexType.VECTOR, PaimonMetadata.toConnectorIndexType("lumina"));
        Assertions.assertEquals(ConnectorIndexType.VECTOR,
                PaimonMetadata.toConnectorIndexType("lumina-vector-ann"));
        Assertions.assertEquals(ConnectorIndexType.FULL_TEXT,
                PaimonMetadata.toConnectorIndexType("tantivy-fulltext"));
        Assertions.assertNull(PaimonMetadata.toConnectorIndexType(null));
        Assertions.assertNull(PaimonMetadata.toConnectorIndexType("unknown"));
    }
}
