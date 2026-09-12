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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.naming.CommunicationException;
import javax.naming.NamingException;
import javax.naming.PartialResultException;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchResult;

import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Active Directory splits a large group's member attribute across range retrieval pages, encoding the
 * page boundaries into the attribute id itself ({@code member;range=0-1499}) - see MS-ADTS 3.1.1.3.1.3.3.
 * A strict {@code attributes.get("member")} therefore finds nothing and the group silently reads as empty.
 */
public class LDAPGroupProviderRangeRetrievalTest {

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
    public void testParseRangeSuffixPlainAttributeReturnsNull() {
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member"));
    }

    @Test
    public void testParseRangeSuffixIntermediatePage() {
        LDAPGroupProvider.RangeInfo info = LDAPGroupProvider.parseRangeSuffix("member;range=0-1499");
        Assertions.assertNotNull(info);
        Assertions.assertEquals(0L, info.start);
        Assertions.assertEquals(1499L, info.end);
        Assertions.assertFalse(info.isTerminal());
    }

    @Test
    public void testParseRangeSuffixTerminalPage() {
        LDAPGroupProvider.RangeInfo info = LDAPGroupProvider.parseRangeSuffix("member;range=1500-*");
        Assertions.assertNotNull(info);
        Assertions.assertEquals(1500L, info.start);
        Assertions.assertTrue(info.isTerminal());
    }

    @Test
    public void testParseRangeSuffixIsCaseInsensitive() {
        LDAPGroupProvider.RangeInfo info = LDAPGroupProvider.parseRangeSuffix("MEMBER;Range=0-1");
        Assertions.assertNotNull(info);
        Assertions.assertEquals(0L, info.start);
        Assertions.assertEquals(1L, info.end);
    }

    @Test
    public void testParseRangeSuffixToleratesOtherAttributeOptions() {
        // AD may carry an option list, e.g. member;lang-en;range=0-1499
        LDAPGroupProvider.RangeInfo info = LDAPGroupProvider.parseRangeSuffix("member;lang-en;range=0-1499");
        Assertions.assertNotNull(info);
        Assertions.assertEquals(0L, info.start);
        Assertions.assertEquals(1499L, info.end);
    }

    @Test
    public void testParseRangeSuffixMalformedReturnsNull() {
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member;range="));
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member;range=abc-xyz"));
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member;blah=0-1"));
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix("member;lang-en"));
        Assertions.assertNull(LDAPGroupProvider.parseRangeSuffix(null));
    }

    // ---------- findMemberAttributeId ----------

    @Test
    public void testFindMemberAttributeIdPlainMatch() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("member"));
        attrs.put(new BasicAttribute("cn", "grp"));

        Assertions.assertEquals("member", LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeIdRangeVariantMatched() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("member;range=0-1499"));
        attrs.put(new BasicAttribute("cn", "grp"));

        Assertions.assertEquals("member;range=0-1499", LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeIdPrefersValuedRangeOverEmptyPlainMember() throws NamingException {
        // AD echoes an empty plain "member" alongside the ranged attribute that actually carries values.
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("member"));
        BasicAttribute range = new BasicAttribute("member;range=0-1");
        range.add("u1");
        attrs.put(range);

        Assertions.assertEquals("member;range=0-1", LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeIdPrefersValuedRangeOverEmptyRangeEcho() throws NamingException {
        // We asked for "2-*"; AD can echo that id with no values and return the real page under "2-3".
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("member;range=2-*"));
        BasicAttribute range = new BasicAttribute("member;range=2-3");
        range.add("u3");
        attrs.put(range);

        Assertions.assertEquals("member;range=2-3", LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeIdCustomMemberAttrIsNotConfusedWithMember() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        BasicAttribute memberUid = new BasicAttribute("memberUid");
        memberUid.add("alice");
        attrs.put(memberUid);

        Assertions.assertEquals("memberUid", LDAPGroupProvider.findMemberAttributeId(attrs, "memberUid"));
        Assertions.assertNull(LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
    }

    @Test
    public void testFindMemberAttributeIdMissingReturnsNull() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("cn", "grp"));

        Assertions.assertNull(LDAPGroupProvider.findMemberAttributeId(attrs, "member"));
        Assertions.assertNull(LDAPGroupProvider.findMemberAttributeId(null, "member"));
    }

    // ---------- consumeMemberPage ----------

    @Test
    public void testConsumeMemberPagePlainAttributeNeedsNoFollowUp() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        BasicAttribute member = new BasicAttribute("member");
        member.add("CN=u1,DC=x");
        member.add("CN=u2,DC=x");
        attrs.put(member);

        List<String> collected = new ArrayList<>();
        long next = LDAPGroupProvider.consumeMemberPage(attrs, "member", collected::add);

        Assertions.assertEquals(-1L, next);
        Assertions.assertEquals(List.of("CN=u1,DC=x", "CN=u2,DC=x"), collected);
    }

    @Test
    public void testConsumeMemberPageTruncatedPageReturnsNextOffset() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        BasicAttribute member = new BasicAttribute("member;range=0-1499");
        member.add("CN=u1,DC=x");
        attrs.put(member);

        List<String> collected = new ArrayList<>();
        Assertions.assertEquals(1500L, LDAPGroupProvider.consumeMemberPage(attrs, "member", collected::add));
        Assertions.assertEquals(List.of("CN=u1,DC=x"), collected);
    }

    @Test
    public void testConsumeMemberPageTerminalPageNeedsNoFollowUp() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        BasicAttribute member = new BasicAttribute("member;range=1500-*");
        member.add("CN=u9,DC=x");
        attrs.put(member);

        List<String> collected = new ArrayList<>();
        Assertions.assertEquals(-1L, LDAPGroupProvider.consumeMemberPage(attrs, "member", collected::add));
        Assertions.assertEquals(List.of("CN=u9,DC=x"), collected);
    }

    @Test
    public void testConsumeMemberPageMissingMemberAttributeIsEmptyNotTruncated() throws NamingException {
        BasicAttributes attrs = new BasicAttributes(true);
        attrs.put(new BasicAttribute("cn", "grp"));

        List<String> collected = new ArrayList<>();
        Assertions.assertEquals(-1L, LDAPGroupProvider.consumeMemberPage(attrs, "member", collected::add));
        Assertions.assertTrue(collected.isEmpty());
    }

    // ---------- fetchRemainingMemberPages ----------

    @Test
    public void testFetchRemainingMemberPagesWalksToTerminalPage() throws NamingException {
        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g,DC=x"), aryEq(new String[] {"member;range=1500-*"})))
                .thenReturn(pagedAttrs("member;range=1500-*", "CN=u4,DC=x", "CN=u5,DC=x"));

        List<String> collected = new ArrayList<>();
        boolean complete = LDAPGroupProvider.fetchRemainingMemberPages(
                ctx, "CN=g,DC=x", "member", 1500L, collected::add);

        Assertions.assertTrue(complete);
        Assertions.assertEquals(List.of("CN=u4,DC=x", "CN=u5,DC=x"), collected);
        verify(ctx, times(1)).getAttributes(eq("CN=g,DC=x"), aryEq(new String[] {"member;range=1500-*"}));
    }

    @Test
    public void testFetchRemainingMemberPagesChainsAcrossServerCappedPages() throws NamingException {
        // The client asks for "3-*" but the server answers "3-5", still non-terminal.
        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g"), aryEq(new String[] {"member;range=3-*"})))
                .thenReturn(pagedAttrs("member;range=3-5", "u4", "u5", "u6"));
        when(ctx.getAttributes(eq("CN=g"), aryEq(new String[] {"member;range=6-*"})))
                .thenReturn(pagedAttrs("member;range=6-*", "u7"));

        List<String> collected = new ArrayList<>();
        boolean complete = LDAPGroupProvider.fetchRemainingMemberPages(ctx, "CN=g", "member", 3L, collected::add);

        Assertions.assertTrue(complete);
        Assertions.assertEquals(List.of("u4", "u5", "u6", "u7"), collected);
    }

    @Test
    public void testFetchRemainingMemberPagesReportsIncompleteOnNamingException() throws NamingException {
        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g"), any(String[].class)))
                .thenThrow(new CommunicationException("simulated transient ldap error"));

        List<String> collected = new ArrayList<>();
        boolean complete = LDAPGroupProvider.fetchRemainingMemberPages(ctx, "CN=g", "member", 2L, collected::add);

        // A truncated member list must be reported as incomplete so the caller refuses to publish it.
        Assertions.assertFalse(complete);
        Assertions.assertTrue(collected.isEmpty());
    }

    @Test
    public void testFetchRemainingMemberPagesReportsIncompleteOnPartialResult() throws NamingException {
        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g"), any(String[].class)))
                .thenThrow(new PartialResultException("referral"));

        Assertions.assertFalse(LDAPGroupProvider.fetchRemainingMemberPages(
                ctx, "CN=g", "member", 2L, m -> { }));
    }

    @Test
    public void testFetchRemainingMemberPagesReportsIncompleteWhenServerSkipsRequestedPage() throws NamingException {
        DirContext ctx = mock(DirContext.class);
        BasicAttributes noMember = new BasicAttributes(true);
        noMember.put(new BasicAttribute("cn", "grp"));
        when(ctx.getAttributes(eq("CN=g"), any(String[].class))).thenReturn(noMember);

        Assertions.assertFalse(LDAPGroupProvider.fetchRemainingMemberPages(
                ctx, "CN=g", "member", 2L, m -> { }));
    }

    @Test
    public void testFetchRemainingMemberPagesReportsIncompleteWhenServerDoesNotAdvance() throws NamingException {
        // A server that keeps answering with the same window would otherwise spin forever.
        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=g"), any(String[].class)))
                .thenReturn(pagedAttrs("member;range=0-1", "u1"));

        Assertions.assertFalse(LDAPGroupProvider.fetchRemainingMemberPages(
                ctx, "CN=g", "member", 2L, m -> { }));
    }

    @Test
    public void testFetchRemainingMemberPagesHonoursMaxRangePages() throws NamingException {
        LDAPGroupProvider.MAX_RANGE_PAGES = 3;

        DirContext ctx = mock(DirContext.class);
        // Always non-terminal, always advancing by one: only the safety limit can stop this.
        when(ctx.getAttributes(eq("g"), any(String[].class))).thenAnswer(invocation -> {
            String[] requestedAttrs = invocation.getArgument(1);
            long start = LDAPGroupProvider.parseRangeSuffix(requestedAttrs[0]).start;
            return pagedAttrs("member;range=" + start + "-" + start, "u" + start);
        });

        List<String> collected = new ArrayList<>();
        boolean complete = LDAPGroupProvider.fetchRemainingMemberPages(ctx, "g", "member", 1L, collected::add);

        Assertions.assertFalse(complete);
        Assertions.assertEquals(List.of("u1", "u2", "u3"), collected);
        verify(ctx, times(3)).getAttributes(eq("g"), any(String[].class));
    }

    @Test
    public void testFetchRemainingMemberPagesWithoutGroupDnMakesNoRequest() throws NamingException {
        DirContext ctx = mock(DirContext.class);

        Assertions.assertFalse(LDAPGroupProvider.fetchRemainingMemberPages(ctx, "", "member", 2L, m -> { }));
        verify(ctx, never()).getAttributes(any(String.class), any(String[].class));
    }

    // ---------- regression baseline for the defect this fixes ----------

    @Test
    public void testLargeGroupIsReadCompletelyAcrossThreePages() throws NamingException {
        // 3500 members, MaxValRange 1500. Before range retrieval support this group read as empty.
        DirContext ctx = mock(DirContext.class);
        when(ctx.getAttributes(eq("CN=big,DC=x"), aryEq(new String[] {"member;range=1500-*"})))
                .thenReturn(pagedAttrs("member;range=1500-2999", members(1500, 3000)));
        when(ctx.getAttributes(eq("CN=big,DC=x"), aryEq(new String[] {"member;range=3000-*"})))
                .thenReturn(pagedAttrs("member;range=3000-*", members(3000, 3500)));

        List<String> collected = new ArrayList<>();
        BasicAttributes firstPage = pagedAttrs("member;range=0-1499", members(0, 1500));

        long next = LDAPGroupProvider.consumeMemberPage(firstPage, "member", collected::add);
        Assertions.assertEquals(1500L, next);
        Assertions.assertTrue(LDAPGroupProvider.fetchRemainingMemberPages(
                ctx, "CN=big,DC=x", "member", next, collected::add));

        Assertions.assertEquals(3500, collected.size());
        Assertions.assertEquals("CN=u0,DC=x", collected.get(0));
        Assertions.assertEquals("CN=u3499,DC=x", collected.get(3499));
    }

    // ---------- extractGroupDN / qualifyRelativeDN ----------

    @Test
    public void testExtractGroupDNQualifiesRelativeNameWithBaseDN() {
        SearchResult result = new SearchResult("CN=g,OU=Groups", null, new BasicAttributes(), true);

        Assertions.assertEquals("CN=g,OU=Groups,DC=x,DC=y",
                LDAPGroupProvider.extractGroupDN(result, "DC=x,DC=y"));
    }

    @Test
    public void testExtractGroupDNLeavesAbsoluteNameAlone() {
        SearchResult result = new SearchResult("CN=g,DC=x,DC=y", null, new BasicAttributes(), false);

        Assertions.assertEquals("CN=g,DC=x,DC=y", LDAPGroupProvider.extractGroupDN(result, "DC=x,DC=y"));
    }

    @Test
    public void testQualifyRelativeDNDoesNotDuplicateBaseDN() {
        Assertions.assertEquals("CN=g,DC=x,DC=y", LDAPGroupProvider.qualifyRelativeDN("CN=g,DC=x,DC=y", "DC=x,DC=y"));
        // The DN suffix comparison is case-insensitive, as DNs are.
        Assertions.assertEquals("CN=g,dc=x,dc=y", LDAPGroupProvider.qualifyRelativeDN("CN=g,dc=x,dc=y", "DC=x,DC=y"));
    }

    @Test
    public void testQualifyRelativeDNStripsQuotesAndWhitespace() {
        Assertions.assertEquals("CN=g,DC=x", LDAPGroupProvider.qualifyRelativeDN(" \"CN=g\" ", " 'DC=x' "));
    }

    // ---------- helpers ----------

    private static BasicAttributes pagedAttrs(String attrId, String... values) {
        BasicAttributes attrs = new BasicAttributes(true);
        BasicAttribute attr = new BasicAttribute(attrId);
        for (String value : values) {
            attr.add(value);
        }
        attrs.put(attr);
        return attrs;
    }

    private static BasicAttributes pagedAttrs(String attrId, List<String> values) {
        return pagedAttrs(attrId, values.toArray(new String[0]));
    }

    private static List<String> members(int fromInclusive, int toExclusive) {
        return IntStream.range(fromInclusive, toExclusive)
                .mapToObj(i -> "CN=u" + i + ",DC=x")
                .collect(Collectors.toList());
    }
}
