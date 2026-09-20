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

package com.starrocks.epack.connector.lakeformation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3LocationTest {

    private static boolean under(String child, String root) {
        return S3Location.parse(child).isSameOrDescendantOf(S3Location.parse(root));
    }

    private static LakeFormationTableAccessException expectRejected(String raw) {
        return assertThrows(LakeFormationTableAccessException.class, () -> S3Location.parse(raw));
    }

    /**
     * The reason this class exists rather than a startsWith call: the sibling below shares a string
     * prefix with the root but is a different table, and letting a table credential reach it would be
     * a cross-table read.
     */
    @Test
    public void testPrefixSiblingIsNotADescendant() {
        assertFalse(under("s3://bucket/table2/part=1", "s3://bucket/table"));
        assertFalse(under("s3://bucket/table2", "s3://bucket/table"));
        assertFalse(under("s3://bucket/tableextra/f", "s3://bucket/table"));
        assertTrue(under("s3://bucket/table/part=1", "s3://bucket/table"));
    }

    @Test
    public void testSelfIsADescendant() {
        assertTrue(under("s3://bucket/table", "s3://bucket/table"));
        assertTrue(under("s3://bucket/table/", "s3://bucket/table"));
    }

    @Test
    public void testTrailingAndDoubledSlashesAreInsignificant() {
        assertEquals(S3Location.parse("s3://bucket/table"), S3Location.parse("s3://bucket/table/"));
        assertEquals(S3Location.parse("s3://bucket/table"), S3Location.parse("s3://bucket//table//"));
        assertTrue(under("s3://bucket/table//part=1/", "s3://bucket/table/"));
    }

    @Test
    public void testSchemeIsNormalizedButOnlyForKnownSchemes() {
        assertEquals(S3Location.parse("s3://bucket/t"), S3Location.parse("s3a://bucket/t"));
        assertEquals(S3Location.parse("s3://bucket/t"), S3Location.parse("S3N://bucket/t"));
        assertTrue(under("s3a://bucket/table/part=1", "s3://bucket/table"));

        assertTrue(expectRejected("hdfs://bucket/t").getMessage().contains("scheme"));
        assertTrue(expectRejected("file:///tmp/t").getMessage().contains("scheme"));
        assertTrue(expectRejected("/bucket/t").getMessage().contains("scheme"));
    }

    @Test
    public void testBucketIsCaseInsensitiveButKeyIsNot() {
        // S3 bucket names cannot differ by case, so folding them is safe; object keys can.
        assertEquals(S3Location.parse("s3://Bucket/t"), S3Location.parse("s3://bucket/t"));
        assertFalse(under("s3://bucket/Table/part=1", "s3://bucket/table"));
    }

    @Test
    public void testCrossBucketIsNeverADescendant() {
        assertFalse(under("s3://other/table/part=1", "s3://bucket/table"));
    }

    @Test
    public void testDotSegmentsAreResolvedAndEscapesRejected() {
        assertEquals(S3Location.parse("s3://bucket/table"), S3Location.parse("s3://bucket/./table"));
        assertEquals(S3Location.parse("s3://bucket/table"), S3Location.parse("s3://bucket/other/../table"));

        // Traversal that lands outside the root must not be waved through as a descendant.
        assertFalse(under("s3://bucket/table/../other", "s3://bucket/table"));
        assertTrue(expectRejected("s3://bucket/../escape").getMessage().contains("above the bucket root"));
    }

    /**
     * The encoded forms of ".." have to traverse exactly like the bare one. Deciding on the raw text
     * and decoding afterwards would leave "%2E%2E" in the list as an ordinary name, so a location that
     * reads as a child of the table root here would resolve outside it wherever the key is decoded
     * later - a containment answer that is wrong in the unsafe direction.
     */
    @Test
    public void testEncodedDotSegmentsTraverseLikeTheBareOnes() {
        assertEquals(S3Location.parse("s3://bucket/table"), S3Location.parse("s3://bucket/other/%2E%2E/table"));
        assertEquals(S3Location.parse("s3://bucket/table"), S3Location.parse("s3://bucket/other/%2e%2e/table"));
        assertEquals(S3Location.parse("s3://bucket/table"), S3Location.parse("s3://bucket/other/%2E./table"));
        assertEquals(S3Location.parse("s3://bucket/table"), S3Location.parse("s3://bucket/%2E/table"));

        assertFalse(under("s3://bucket/table/%2E%2E/other/x", "s3://bucket/table"));
        assertFalse(under("s3://bucket/table/%2e%2e/other/x", "s3://bucket/table"));
        assertTrue(expectRejected("s3://bucket/%2E%2E/escape").getMessage().contains("above the bucket root"));
    }

    /**
     * A literal space is not legal in a URI but is perfectly legal in an S3 key, and Glue hands back
     * such locations verbatim. Rejecting them would fail closed on ordinary tables, so the parser has
     * to accept the raw form and the encoded form as the same location.
     */
    @Test
    public void testPercentEncodingIsDecodedButEncodedSeparatorsAreRejected() {
        assertEquals(S3Location.parse("s3://bucket/my table"), S3Location.parse("s3://bucket/my%20table"));
        assertTrue(under("s3://bucket/my table/part=1", "s3://bucket/my table"));
        assertTrue(under("s3://bucket/tbl/dt=2026%2D01%2D01", "s3://bucket/tbl"));

        // An encoded separator is ambiguous depending on when it is decoded, so it is refused rather
        // than resolved one way or the other.
        assertTrue(expectRejected("s3://bucket/a%2Fb").getMessage().contains("encoded path separator"));
        assertTrue(expectRejected("s3://bucket/a%2fb").getMessage().contains("encoded path separator"));
    }

    /**
     * A plus is a plus, not a space. URLDecoder would read it as form-encoded and fold the two
     * together, which would let a partition under "tbl+a" pass as living under the root "tbl a".
     */
    @Test
    public void testPlusIsNotASpace() {
        assertNotEquals(S3Location.parse("s3://bucket/a+b"), S3Location.parse("s3://bucket/a b"));
        assertFalse(under("s3://bucket/tbl+a/part=1", "s3://bucket/tbl a"));
        assertTrue(under("s3://bucket/tbl+a/part=1", "s3://bucket/tbl+a"));
    }

    /**
     * A literal percent is legal in an S3 key, so a malformed escape has to come back as our own
     * exception type - a raw IllegalArgumentException would slip past the fail-closed handlers that
     * match on LakeFormationTableAccessException.
     */
    @Test
    public void testMalformedPercentEscapesAreRefusedWithOurOwnType() {
        assertTrue(expectRejected("s3://bucket/100%done").getMessage().contains("malformed percent escape"));
        assertTrue(expectRejected("s3://bucket/a%zz").getMessage().contains("malformed percent escape"));
        assertTrue(expectRejected("s3://bucket/trailing%").getMessage().contains("truncated percent escape"));
        assertTrue(expectRejected("s3://bucket/trailing%2").getMessage().contains("truncated percent escape"));
    }

    /** Multi-byte characters have to survive the decode, or two paths would compare unequal by accident. */
    @Test
    public void testMultiByteEscapesRoundTrip() {
        assertEquals(S3Location.parse("s3://bucket/数据"), S3Location.parse("s3://bucket/%E6%95%B0%E6%8D%AE"));
        assertTrue(under("s3://bucket/%E6%95%B0%E6%8D%AE/dt=1", "s3://bucket/数据"));
    }

    @Test
    public void testMalformedLocationsAreRejected() {
        assertTrue(expectRejected(null).getMessage().contains("empty"));
        assertTrue(expectRejected("   ").getMessage().contains("empty"));
        assertTrue(expectRejected("s3://").getMessage().contains("no bucket"));
        assertTrue(expectRejected("s3://bucket:9000/t").getMessage().contains("port"));
        assertTrue(expectRejected("s3://user@bucket/t").getMessage().contains("user info"));
        assertTrue(expectRejected("s3://bucket/t?versionId=1").getMessage().contains("query string"));
        assertTrue(expectRejected("s3://bucket/t#frag").getMessage().contains("fragment"));
        assertTrue(expectRejected("bucket/t").getMessage().contains("no scheme"));
    }

    @Test
    public void testBucketRootIsAValidRoot() {
        assertTrue(under("s3://bucket/anything/deep", "s3://bucket"));
        assertTrue(under("s3://bucket", "s3://bucket/"));
        assertFalse(under("s3://other", "s3://bucket"));
    }

    /**
     * The other direction of the containment check: a table root is not a descendant of one of its own
     * partitions. Getting this backwards would let a partition-scoped credential cover the whole table.
     */
    @Test
    public void testAShorterPathIsNotADescendantOfALongerOne() {
        assertFalse(under("s3://bucket/table", "s3://bucket/table/part=1"));
        assertTrue(under("s3://bucket/table/part=1", "s3://bucket/table"));
    }

    @Test
    public void testParsedPartsAreReadBackNormalized() {
        S3Location location = S3Location.parse("s3://BUCKET/Table//part=1/");
        assertEquals("bucket", location.bucket());
        // Only the bucket is folded: keys are case-sensitive in S3, so folding them would be a bug.
        assertEquals(java.util.List.of("Table", "part=1"), location.segments());
        assertEquals("s3://bucket/Table/part=1", location.toString());
    }

    @Test
    public void testEqualityIsByBucketAndSegments() {
        S3Location location = S3Location.parse("s3://bucket/table/part=1");
        assertEquals(location, location);
        assertEquals(location, S3Location.parse("s3://bucket/table//part=1/"));
        assertEquals(location.hashCode(), S3Location.parse("s3://bucket/table/part=1").hashCode());
        assertNotEquals(location, S3Location.parse("s3://bucket/table"));
        assertNotEquals(location, S3Location.parse("s3://other/table/part=1"));
        // A raw string is never equal to a parsed location, however identical it looks.
        assertNotEquals(location, "s3://bucket/table/part=1");
    }
}
