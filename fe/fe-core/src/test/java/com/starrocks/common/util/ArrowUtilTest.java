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

package com.starrocks.common.util;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

public class ArrowUtilTest {

    // Non-ASCII text: Polish and Japanese
    private static final String NON_ASCII = "zażółć 日本語";

    // Arrow's Utf8 vectors hold UTF-8, but the bytes used to come from String.getBytes() in the JVM's default charset,
    // which is ASCII in a container without a locale and turned non-ASCII text into '?'.
    @Test
    public void testCreateSingleSchemaRootEncodesValueAsUtf8() {
        try (MockedConstruction<VarCharVector> vectors = mockConstruction(VarCharVector.class);
                MockedStatic<VectorSchemaRoot> roots =
                        mockStatic(VectorSchemaRoot.class, invocation -> mock(VectorSchemaRoot.class))) {
            ArrowUtil.createSingleSchemaRoot("col1", NON_ASCII);

            assertEquals(1, vectors.constructed().size());
            VarCharVector vector = vectors.constructed().get(0);
            verify(vector).allocateNew();
            verify(vector).setSafe(0, NON_ASCII.getBytes(StandardCharsets.UTF_8));
            verify(vector).setValueCount(1);
        }
    }
}
