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

package com.starrocks.http.rest.context;

import com.starrocks.context.retrieval.ContextSearchExecutor;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the fusion-weight resolution contract of {@code POST /api/context/search}.
 *
 * <p>{@link ContextSearchExecutor.Request} constructs with the vector-dominant defaults
 * ({@code DEFAULT_TEXT_WEIGHT=0.1 / DEFAULT_VECTOR_WEIGHT=0.6 / DEFAULT_GRAPH_WEIGHT=0.3}). The REST
 * layer must only override a weight the caller actually supplied — when a key is absent the field
 * must stay at the executor default and remain non-explicit, so a retrieval profile can still set
 * it downstream. A literal fallback in the REST layer silently shadows the executor default and
 * pins every weightless call to that literal; these tests fail while that bug is present.
 */
public class ContextSearchActionWeightsTest {

    /**
     * Core reproduction: a body with no weight keys must leave all three weights at the executor's
     * vector-dominant defaults. With the {@code getOrDefault("text_weight", 0.5)}-style fallback in
     * place this fails — textWeight resolves to 0.5, vectorWeight to 0.3, graphWeight to 0.2.
     */
    @Test
    public void testWeightlessRequestKeepsVectorDominantDefaults() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contextbase", "cb1");
        payload.put("query_text", "deal scoring");

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        ContextSearchAction.applyWeights(payload, req);

        assertEquals(ContextSearchExecutor.DEFAULT_TEXT_WEIGHT, req.textWeight, 1e-9);
        assertEquals(ContextSearchExecutor.DEFAULT_VECTOR_WEIGHT, req.vectorWeight, 1e-9);
        assertEquals(ContextSearchExecutor.DEFAULT_GRAPH_WEIGHT, req.graphWeight, 1e-9);
        assertFalse(req.explicitTextWeight, "absent text_weight must not be marked explicit");
        assertFalse(req.explicitVectorWeight, "absent vector_weight must not be marked explicit");
        assertFalse(req.explicitGraphWeight, "absent graph_weight must not be marked explicit");
    }

    /**
     * The most insidious real-world shape: a caller passes only {@code text_weight}, expecting the
     * unspecified vector/graph weights to keep their vector-dominant defaults. The literal-fallback
     * bug instead clobbers vector to 0.3 and graph to 0.2.
     */
    @Test
    public void testPartialWeightsLeaveUnsetWeightsAtDefault() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("text_weight", 0.2);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        ContextSearchAction.applyWeights(payload, req);

        assertEquals(0.2, req.textWeight, 1e-9);
        assertTrue(req.explicitTextWeight);
        assertEquals(ContextSearchExecutor.DEFAULT_VECTOR_WEIGHT, req.vectorWeight, 1e-9);
        assertEquals(ContextSearchExecutor.DEFAULT_GRAPH_WEIGHT, req.graphWeight, 1e-9);
        assertFalse(req.explicitVectorWeight);
        assertFalse(req.explicitGraphWeight);
    }

    /**
     * Control: when the caller supplies all three weights they must flow through verbatim and be
     * marked explicit. This passes both before and after the fix — it guards against a fix that
     * over-corrects and starts ignoring caller-supplied weights.
     */
    @Test
    public void testExplicitWeightsAreHonoredAndMarkedExplicit() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("text_weight", 0.4);
        payload.put("vector_weight", 0.4);
        payload.put("graph_weight", 0.2);

        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        ContextSearchAction.applyWeights(payload, req);

        assertEquals(0.4, req.textWeight, 1e-9);
        assertEquals(0.4, req.vectorWeight, 1e-9);
        assertEquals(0.2, req.graphWeight, 1e-9);
        assertTrue(req.explicitTextWeight);
        assertTrue(req.explicitVectorWeight);
        assertTrue(req.explicitGraphWeight);
    }
}
