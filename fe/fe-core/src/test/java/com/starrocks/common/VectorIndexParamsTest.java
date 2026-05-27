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

package com.starrocks.common;

import com.starrocks.common.VectorIndexParams.CommonIndexParamKey;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VectorIndexParamsTest {

    @Test
    public void testIndexBuildModeAcceptsSyncAndAsync() {
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.INDEX_BUILD_MODE.check("sync"));
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.INDEX_BUILD_MODE.check("async"));
    }

    @Test
    public void testIndexBuildModeIsCaseInsensitive() {
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.INDEX_BUILD_MODE.check("SYNC"));
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.INDEX_BUILD_MODE.check("Async"));
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.INDEX_BUILD_MODE.check("ASYNC"));
    }

    @Test
    public void testIndexBuildModeRejectsInvalidValue() {
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> CommonIndexParamKey.INDEX_BUILD_MODE.check("lazy"));
        Assertions.assertTrue(ex.getMessage().contains("sync"));
        Assertions.assertTrue(ex.getMessage().contains("async"));
    }

    @Test
    public void testIndexBuildModeRejectsEmptyValue() {
        Assertions.assertThrows(SemanticException.class,
                () -> CommonIndexParamKey.INDEX_BUILD_MODE.check(""));
    }

    @Test
    public void testIndexTypeAcceptsKnownTypes() {
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.INDEX_TYPE.check("HNSW"));
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.INDEX_TYPE.check("ivfpq"));
    }

    @Test
    public void testIndexTypeRejectsUnknown() {
        Assertions.assertThrows(SemanticException.class,
                () -> CommonIndexParamKey.INDEX_TYPE.check("flat"));
    }

    @Test
    public void testMetricTypeAcceptsKnown() {
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.METRIC_TYPE.check("cosine_similarity"));
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.METRIC_TYPE.check("L2_DISTANCE"));
    }

    @Test
    public void testMetricTypeRejectsUnknown() {
        Assertions.assertThrows(SemanticException.class,
                () -> CommonIndexParamKey.METRIC_TYPE.check("manhattan"));
    }

    @Test
    public void testDimRejectsNonInteger() {
        Assertions.assertThrows(SemanticException.class,
                () -> CommonIndexParamKey.DIM.check("abc"));
    }

    @Test
    public void testDimRejectsZero() {
        Assertions.assertThrows(SemanticException.class,
                () -> CommonIndexParamKey.DIM.check("0"));
    }

    @Test
    public void testIsVectorNormedAcceptsBooleanStrings() {
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.IS_VECTOR_NORMED.check("true"));
        Assertions.assertDoesNotThrow(() -> CommonIndexParamKey.IS_VECTOR_NORMED.check("FALSE"));
    }

    @Test
    public void testIsVectorNormedRejectsNonBoolean() {
        Assertions.assertThrows(SemanticException.class,
                () -> CommonIndexParamKey.IS_VECTOR_NORMED.check("yes"));
    }
}
