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

package com.starrocks.authentication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import javax.naming.NamingException;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchResult;

import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for Active Directory range retrieval ({@code member;range=N-M}) handling in
 * {@link LDAPGroupProvider}. The static helpers introduced for range parsing and paging are
 * verified directly so that the regression behavior is documented as code.
 */
class LDAPGroupProviderRangeRetrievalTest {

    private int originalMaxRangePages;

    @BeforeEach
    public void setUp() {
        originalMaxRangePages = LDAPGroupProvider.MAX_RANGE_PAGES;
    }

    @AfterEach
    public void tearDown() {
        LDAPGroupProvider.MAX_RANGE_PAGES = originalMaxRangePages;
    }

    // ---------- parseRangeSuffix ----------

    @Test
    public void testParseRangeSuffix_plainAttributeReturnsNull() {
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member"));
    }

    @Test
    public void testParseRangeSuffix_intermediatePage() {
        LDAPGroupProvider.RangeInfo info = LDAPGroupProvider.parseRangeSuffix("member;range=0-1499");
        Assertions.assertNotNull(info);
        Assertions.assertEquals(0L, info.start);
        Assertions.assertEquals(1499L, info.end);
        Assertions.assertFalse(info.isTerminal());
    }

    @Test
    public void testParseRangeSuffix_terminalPage() {
        LDAPGroupProvider.RangeInfo info = LDAPGroupProvider.parseRangeSuffix("member;range=1500-*");
        Assertions.assertNotNull(info);
        Assertions.assertEquals(1500L, info.start);
        Assertions.assertEquals(-1L, info.end);
        Assertions.assertTrue(info.isTerminal());
    }

    @Test
    public void testParseRangeSuffix_caseInsensitive() {
        LDAPGroupProvider.RangeInfo info = LDAPGroupProvider.parseRangeSuffix("MEMBER;Range=0-1");
        Assertions.assertNotNull(info);
        Assertions.assertEquals(0L, info.start);
        Assertions.assertEquals(1L, info.end);
    }

    @Test
    public void testParseRangeSuffix_malformedSuffixReturnsNull() {
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member;range="));
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member;range=abc-xyz"));
    }

    @Test
    public void testParseRangeSuffix_unrelatedSuffixReturnsNull() {
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member;blah=0-1"));
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member;lang-en"));
    }

    @Test
    public void testParseRangeSuffix_nullReturnsNull() {
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix(null));
    }

    // ---------- findMemberAttributeId ----------

    @Test
    public void testFindMemberAttributeId_plainMatchTakesPrecedence() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("member"));
        attrs.put(new BasicAttribute("cn", "grp"));

        Assertions.assertEquals("member", LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeId_rangeVariantMatched() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("member;range=0-1499"));
        attrs.put(new BasicAttribute("cn", "grp"));

        Assertions.assertEquals("member;range=0-1499",
                LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeId_valuedRangePreferredOverEmptyPlainMember() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("member"));
        BasicAttribute range = new BasicAttribute("member;range=0-1");
        range.add("u1");
        attrs.put(range);

        Assertions.assertEquals("member;range=0-1",
                LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeId_rangeOptionAfterOtherOptionMatched() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        BasicAttribute range = new BasicAttribute("member;lang-en;range=0-1");
        range.add("u1");
        attrs.put(range);

        Assertions.assertEquals("member;lang-en;range=0-1",
                LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeId_caseInsensitiveRangeMatch() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(false);
        attrs.put(new BasicAttribute("MEMBER;Range=0-1"));

        Assertions.assertEquals("MEMBER;Range=0-1",
                LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeId_unrelatedAttributeIgnored() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("cn", "grp"));
        attrs.put(new BasicAttribute("description", "test group"));

        Assertions.assertNull(LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeId_nullAttributesReturnsNull() throws NamingException {
        Assertions.assertNull(LDAPGroupProvider.findMemberAttributeId(null, "member"));
    }

    // ---------- collectAllMembers (range paging end-to-end) ----------

    @Test
    public void testCollectAllMembers_plainResponseSinglePage() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        BasicAttribute member = new BasicAttribute("member");
        member.add("CN=u1,OU=People,DC=x,DC=y");
        member.add("CN=u2,OU=People,DC=x,DC=y");
        attrs.put(member);

        DirContext ctx = mock(DirContext.class);
        List<String> collected = new ArrayList<>();

        LDAPGroupProvider.collectAllMembers(ctx, "CN=g,DC=x,DC=y", "member", attrs, collected::add);

        Assertions.assertEquals(List.of("CN=u1,OU=People,DC=x,DC=y", "CN=u2,OU=People,DC=x,DC=y"),
                collected);
        // No follow-up page request expected.
        verify(ctx, times(0)).getAttributes(eq("CN=g,DC=x,DC=y"),
                aryEq(new String[] {"member;range=2-*"}));
    }

    @Test
    public void testCollectAllMembers_pagedThenTerminal() throws NamingException {
        // Page 1: member;range=0-2 with 3 entries
        BasicAttributes page1 = new BasicAttributes(true);
        BasicAttribute p1Attr = new BasicAttribute("member;range=0-2");
        p1Attr.add("CN=u1,DC=x");
        p1Attr.add("CN=u2,DC=x");
        p1Attr.add("CN=u3,DC=x");
        page1.put(p1Attr);

        // Page 2: member;range=3-* with 2 entries (terminal)
        BasicAttributes page2 = new BasicAttributes(true);
        BasicAttribute p2Attr = new BasicAttribute("member;range=3-*");
        p2Attr.add("CN=u4,DC=x");
        p2Attr.add("CN=u5,DC=x");
        page2.put(p2Attr);

        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g,DC=x"), aryEq(new String[] {"member;range=3-*"})))
                .thenReturn(page2);

        List<String> collected = new ArrayList<>();
        LDAPGroupProvider.collectAllMembers(ctx, "CN=g,DC=x", "member", page1, collected::add);

        Assertions.assertEquals(
                List.of("CN=u1,DC=x", "CN=u2,DC=x", "CN=u3,DC=x", "CN=u4,DC=x", "CN=u5,DC=x"),
                collected);
        verify(ctx, times(1)).getAttributes(eq("CN=g,DC=x"),
                aryEq(new String[] {"member;range=3-*"}));
    }

    @Test
    public void testCollectAllMembers_initialAdResponseSkipsEmptyPlainMember() throws NamingException {
        // AD may return an empty plain "member" plus the actual ranged attribute.
        BasicAttributes page1 = new BasicAttributes(true);
        page1.put(new BasicAttribute("member"));
        BasicAttribute p1Attr = new BasicAttribute("member;range=0-1");
        p1Attr.add("u1");
        p1Attr.add("u2");
        page1.put(p1Attr);

        BasicAttributes page2 = new BasicAttributes(true);
        BasicAttribute p2Attr = new BasicAttribute("member;range=2-*");
        p2Attr.add("u3");
        page2.put(p2Attr);

        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g"), aryEq(new String[] {"member;range=2-*"})))
                .thenReturn(page2);

        List<String> collected = new ArrayList<>();
        LDAPGroupProvider.collectAllMembers(ctx, "CN=g", "member", page1, collected::add);

        Assertions.assertEquals(List.of("u1", "u2", "u3"), collected);
    }

    @Test
    public void testCollectAllMembers_followupAdResponseSkipsEmptyRequestedRange() throws NamingException {
        BasicAttributes page1 = new BasicAttributes(true);
        BasicAttribute p1Attr = new BasicAttribute("member;range=0-1");
        p1Attr.add("u1");
        p1Attr.add("u2");
        page1.put(p1Attr);

        // Client asks for "2-*"; AD can echo that request attribute with no values
        // and return the server-capped real page under "2-3".
        BasicAttributes page2 = new BasicAttributes(true);
        page2.put(new BasicAttribute("member;range=2-*"));
        BasicAttribute p2Attr = new BasicAttribute("member;range=2-3");
        p2Attr.add("u3");
        p2Attr.add("u4");
        page2.put(p2Attr);

        BasicAttributes page3 = new BasicAttributes(true);
        BasicAttribute p3Attr = new BasicAttribute("member;range=4-*");
        p3Attr.add("u5");
        page3.put(p3Attr);

        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g"), aryEq(new String[] {"member;range=2-*"})))
                .thenReturn(page2);
        when(ctx.getAttributes(eq("CN=g"), aryEq(new String[] {"member;range=4-*"})))
                .thenReturn(page3);

        List<String> collected = new ArrayList<>();
        LDAPGroupProvider.collectAllMembers(ctx, "CN=g", "member", page1, collected::add);

        Assertions.assertEquals(List.of("u1", "u2", "u3", "u4", "u5"), collected);
    }

    @Test
    public void testCollectAllMembers_threePagesAcrossNonTerminalMiddle() throws NamingException {
        // Server-side capped: client requests "3-*" but server replies "3-5" (still non-terminal),
        // then on the next request "6-*" replies terminal.
        BasicAttributes page1 = new BasicAttributes(true);
        BasicAttribute p1 = new BasicAttribute("member;range=0-2");
        p1.add("u1");
        p1.add("u2");
        p1.add("u3");
        page1.put(p1);

        BasicAttributes page2 = new BasicAttributes(true);
        BasicAttribute p2 = new BasicAttribute("member;range=3-5");
        p2.add("u4");
        p2.add("u5");
        p2.add("u6");
        page2.put(p2);

        BasicAttributes page3 = new BasicAttributes(true);
        BasicAttribute p3 = new BasicAttribute("member;range=6-*");
        p3.add("u7");
        page3.put(p3);

        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g"), aryEq(new String[] {"member;range=3-*"})))
                .thenReturn(page2);
        when(ctx.getAttributes(eq("CN=g"), aryEq(new String[] {"member;range=6-*"})))
                .thenReturn(page3);

        List<String> collected = new ArrayList<>();
        LDAPGroupProvider.collectAllMembers(ctx, "CN=g", "member", page1, collected::add);

        Assertions.assertEquals(List.of("u1", "u2", "u3", "u4", "u5", "u6", "u7"), collected);
    }

    // ---------- safety limit ----------

    @Test
    public void testCollectAllMembers_maxRangePagesSafetyLimit() throws NamingException {
        LDAPGroupProvider.MAX_RANGE_PAGES = 3;

        // Page 0 (initial) is non-terminal: server keeps advertising non-terminal indefinitely.
        BasicAttributes page0 = new BasicAttributes(true);
        BasicAttribute p0 = new BasicAttribute("member;range=0-0");
        p0.add("u0");
        page0.put(p0);

        // Subsequent pages: always non-terminal "member;range=N-N" with one entry.
        BasicAttributes p1 = makePagedAttrs("member;range=1-1", "u1");
        BasicAttributes p2 = makePagedAttrs("member;range=2-2", "u2");
        BasicAttributes p3 = makePagedAttrs("member;range=3-3", "u3");
        BasicAttributes p4 = makePagedAttrs("member;range=4-4", "u4");

        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("g"), aryEq(new String[] {"member;range=1-*"}))).thenReturn(p1);
        when(ctx.getAttributes(eq("g"), aryEq(new String[] {"member;range=2-*"}))).thenReturn(p2);
        when(ctx.getAttributes(eq("g"), aryEq(new String[] {"member;range=3-*"}))).thenReturn(p3);
        when(ctx.getAttributes(eq("g"), aryEq(new String[] {"member;range=4-*"}))).thenReturn(p4);

        List<String> collected = new ArrayList<>();
        LDAPGroupProvider.collectAllMembers(ctx, "g", "member", page0, collected::add);

        // With MAX_RANGE_PAGES=3 we expect at most 1 (initial) + 3 (follow-ups) = 4 pages
        // consumed (u0..u3). The 4th follow-up must not be requested.
        Assertions.assertEquals(List.of("u0", "u1", "u2", "u3"), collected);
        verify(ctx, times(0)).getAttributes(eq("g"), aryEq(new String[] {"member;range=4-*"}));
    }

    // ---------- error resilience ----------

    @Test
    public void testCollectAllMembers_followupNamingExceptionPreservesCollected() throws NamingException {
        BasicAttributes page1 = new BasicAttributes(true);
        BasicAttribute p1 = new BasicAttribute("member;range=0-1");
        p1.add("u1");
        p1.add("u2");
        page1.put(p1);

        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("g"), aryEq(new String[] {"member;range=2-*"})))
                .thenThrow(new NamingException("simulated transient ldap error"));

        List<String> collected = new ArrayList<>();
        LDAPGroupProvider.collectAllMembers(ctx, "g", "member", page1, collected::add);

        // Members collected before the failure must be preserved.
        Assertions.assertEquals(List.of("u1", "u2"), collected);
    }

    @Test
    public void testCollectAllMembers_emptyGroupDNSkipsFollowupRequest() throws NamingException {
        BasicAttributes page1 = new BasicAttributes(true);
        BasicAttribute p1 = new BasicAttribute("member;range=0-1");
        p1.add("u1");
        p1.add("u2");
        page1.put(p1);

        DirContext ctx = mock(DirContext.class);
        List<String> collected = new ArrayList<>();

        // groupDN empty -> we cannot request the next range page, but the initial page must
        // still be consumed.
        LDAPGroupProvider.collectAllMembers(ctx, "", "member", page1, collected::add);

        Assertions.assertEquals(List.of("u1", "u2"), collected);
        verify(ctx, times(0)).getAttributes(eq(""), aryEq(new String[] {"member;range=2-*"}));
    }

    @Test
    public void testCollectAllMembers_missingMemberAttributeReturnsEmpty() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("cn", "grp"));

        DirContext ctx = mock(DirContext.class);
        List<String> collected = new ArrayList<>();

        LDAPGroupProvider.collectAllMembers(ctx, "CN=g,DC=x", "member", attrs, collected::add);

        Assertions.assertTrue(collected.isEmpty());
    }

    // ---------- group DN extraction ----------

    @Test
    public void testExtractGroupDN_relativeNameUsesBaseDN() {
        SearchResult result = new SearchResult("CN=g,OU=Groups", null, new BasicAttributes(), true);

        Assertions.assertEquals("CN=g,OU=Groups,DC=x,DC=y",
                LDAPGroupProvider.extractGroupDN(result, "DC=x,DC=y"));
    }

    @Test
    public void testExtractGroupDN_absoluteNameIsNotQualifiedAgain() {
        SearchResult result = new SearchResult("CN=g,DC=x,DC=y", null, new BasicAttributes(), false);

        Assertions.assertEquals("CN=g,DC=x,DC=y",
                LDAPGroupProvider.extractGroupDN(result, "DC=x,DC=y"));
    }

    // ---------- helpers ----------

    private static BasicAttributes makePagedAttrs(String attrId, String... values) {
        BasicAttributes attrs = new BasicAttributes(true);
        BasicAttribute attr = new BasicAttribute(attrId);
        for (String v : values) {
            attr.add(v);
        }
        attrs.put(attr);
        return attrs;
    }
}
