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

package com.starrocks.context.retrieval;

import com.starrocks.context.ContextMgr;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

/**
 * Covers the multi-contextbase resolution path of {@link ContextScopeResolver#resolveContextBases}
 * and the scope/collection_type validation of {@link ContextScopeResolver#resolve}.
 */
public class ContextScopeResolverTest {

    private static ContextMgr mgrWithBases() {
        ContextMgr mgr = Mockito.mock(ContextMgr.class);
        Mockito.when(mgr.getContextBase("a"))
                .thenReturn(new ContextMgr.ContextBaseMeta(7L, "a", Collections.emptyMap()));
        Mockito.when(mgr.getContextBase("b"))
                .thenReturn(new ContextMgr.ContextBaseMeta(9L, "b", Collections.emptyMap()));
        return mgr;
    }

    @Test
    public void resolvesMultipleBasesToIdList() {
        ContextScopeResolver.ResolvedScope scope = ContextScopeResolver.resolveContextBases(
                mgrWithBases(), Arrays.asList("a", "b"), null, null, null);
        Assertions.assertTrue(scope.isMultiContextBase());
        Assertions.assertNull(scope.contextBase);
        Assertions.assertEquals(Arrays.asList("a", "b"), scope.contextBases);
        Assertions.assertEquals(Arrays.asList(7L, 9L), scope.contextBaseIds);
        Assertions.assertTrue(scope.collectionIds.isEmpty());
    }

    @Test
    public void deduplicatesAndPreservesOrder() {
        ContextScopeResolver.ResolvedScope scope = ContextScopeResolver.resolveContextBases(
                mgrWithBases(), Arrays.asList("a", "b", "a"), null, null, null);
        Assertions.assertEquals(Arrays.asList(7L, 9L), scope.contextBaseIds);
    }

    @Test
    public void singleElementListDelegatesToSingleBasePath() {
        // A 1-element list is not "multi"; collection-level scope is allowed and resolves normally.
        ContextMgr mgr = mgrWithBases();
        Mockito.when(mgr.listCollections("a"))
                .thenReturn(Collections.singletonList(
                        new ContextMgr.CollectionMeta(100L, 7L, "c1", null, Collections.emptyMap())));
        ContextScopeResolver.ResolvedScope scope = ContextScopeResolver.resolveContextBases(
                mgr, Collections.singletonList("a"), "c1", null, null);
        Assertions.assertFalse(scope.isMultiContextBase());
        Assertions.assertEquals("a", scope.contextBase);
        Assertions.assertEquals(Collections.singletonList(100L), scope.collectionIds);
    }

    @Test
    public void multiBaseRejectsCollectionScope() {
        ContextException ex = Assertions.assertThrows(ContextException.class, () ->
                ContextScopeResolver.resolveContextBases(
                        mgrWithBases(), Arrays.asList("a", "b"), "c1", null, null));
        Assertions.assertTrue(ex.getMessage().contains("collection-level scope"), ex.getMessage());
    }

    @Test
    public void unknownBaseFails() {
        ContextException ex = Assertions.assertThrows(ContextException.class, () ->
                ContextScopeResolver.resolveContextBases(
                        mgrWithBases(), Arrays.asList("a", "missing"), null, null, null));
        Assertions.assertTrue(ex.getMessage().contains("contextbase not found"), ex.getMessage());
    }

    @Test
    public void emptyListFails() {
        Assertions.assertThrows(ContextException.class, () ->
                ContextScopeResolver.resolveContextBases(
                        mgrWithBases(), Collections.emptyList(), null, null, null));
    }

    @Test
    public void scopeCombinedWithCollectionRejected() {
        // scope=a.* plus collection=docs would silently run under the scope and drop the
        // collection; the selectors are mutually exclusive, so reject the conflict up front.
        ContextException ex = Assertions.assertThrows(ContextException.class, () ->
                ContextScopeResolver.resolve(
                        mgrWithBases(), "a.*", null, "docs", null, null));
        Assertions.assertEquals(ContextErrorCode.INVALID_SCOPE, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("scope cannot be combined"), ex.getMessage());
    }

    @Test
    public void scopeCombinedWithContextBaseRejected() {
        ContextException ex = Assertions.assertThrows(ContextException.class, () ->
                ContextScopeResolver.resolve(
                        mgrWithBases(), "a.c", "b", null, null, null));
        Assertions.assertEquals(ContextErrorCode.INVALID_SCOPE, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("scope cannot be combined"), ex.getMessage());
    }

    @Test
    public void scopeCombinedWithCollectionTypeRejected() {
        ContextException ex = Assertions.assertThrows(ContextException.class, () ->
                ContextScopeResolver.resolve(
                        mgrWithBases(), "a.*", null, null, null, "knowledge"));
        Assertions.assertEquals(ContextErrorCode.INVALID_SCOPE, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("scope cannot be combined"), ex.getMessage());
    }

    @Test
    public void unknownCollectionTypeRejected() {
        // A typo like collection_type=knowlege must surface as a non-retryable parameter error
        // rather than silently resolving to an empty (zero-result) scope.
        ContextException ex = Assertions.assertThrows(ContextException.class, () ->
                ContextScopeResolver.resolveNames(mgrWithBases(), "a", null, null, "knowlege"));
        Assertions.assertEquals(ContextErrorCode.INVALID_COLLECTION_TYPE, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("unknown collection_type"), ex.getMessage());
    }

    @Test
    public void validCollectionTypeResolvesFilteredCollections() {
        ContextMgr mgr = mgrWithBases();
        Mockito.when(mgr.listCollections("a")).thenReturn(Arrays.asList(
                new ContextMgr.CollectionMeta(100L, 7L, "kb", "knowledge", Collections.emptyMap()),
                new ContextMgr.CollectionMeta(101L, 7L, "mem", "memory", Collections.emptyMap())));
        ContextScopeResolver.ResolvedScope scope = ContextScopeResolver.resolveNames(
                mgr, "a", null, null, "knowledge");
        Assertions.assertEquals("knowledge", scope.collectionType);
        Assertions.assertEquals(Collections.singletonList(100L), scope.collectionIds);
        Assertions.assertEquals(Long.valueOf(100L), scope.collectionId);
    }
}
