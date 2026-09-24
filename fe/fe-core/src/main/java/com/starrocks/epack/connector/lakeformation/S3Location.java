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

import com.google.common.collect.ImmutableList;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A normalized S3 location, split into path segments so containment can be decided per segment.
 *
 * A table credential covers the whole subtree under the table's Location, so "is this partition
 * inside the table root" is a security decision, not a convenience. It must never be answered with
 * String.startsWith: "s3://b/table2" starts with "s3://b/table" and would be waved through as a
 * member of that table's subtree, which is a real cross-table read.
 *
 * Anything that cannot be normalized with certainty is rejected rather than guessed at.
 */
public final class S3Location {
    private static final Set<String> SUPPORTED_SCHEMES = Set.of("s3", "s3a", "s3n");

    private final String bucket;
    private final List<String> segments;

    private S3Location(String bucket, List<String> segments) {
        this.bucket = bucket;
        this.segments = segments;
    }

    public String bucket() {
        return bucket;
    }

    public List<String> segments() {
        return segments;
    }

    /**
     * @throws LakeFormationTableAccessException if the URI cannot be normalized with certainty
     */
    public static S3Location parse(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw reject(String.valueOf(raw), "the location is empty");
        }
        String trimmed = raw.trim();

        // An encoded separator would let "a%2Fb" pose as the two segments "a" and "b" - or hide a
        // separator from the segment split, depending on the order of decoding. Neither is worth
        // resolving: refuse instead.
        if (trimmed.toLowerCase(Locale.ROOT).contains("%2f")) {
            throw reject(trimmed, "it contains an encoded path separator (%2F)");
        }

        // Split by hand rather than through java.net.URI: object keys routinely contain characters
        // the URI grammar rejects outright - a literal space in "s3://bucket/my table/" is the common
        // one - and refusing those would fail closed on perfectly ordinary tables. What matters here
        // is that both locations being compared are normalized the same way, not that either one
        // satisfies RFC 3986.
        int schemeEnd = trimmed.indexOf("://");
        if (schemeEnd < 0) {
            throw reject(trimmed, "it has no scheme; expected one of " + SUPPORTED_SCHEMES);
        }
        String scheme = trimmed.substring(0, schemeEnd).toLowerCase(Locale.ROOT);
        if (!SUPPORTED_SCHEMES.contains(scheme)) {
            throw reject(trimmed, "the scheme " + scheme + " is not one of " + SUPPORTED_SCHEMES);
        }

        String rest = trimmed.substring(schemeEnd + 3);
        int bucketEnd = rest.indexOf('/');
        String bucket = bucketEnd < 0 ? rest : rest.substring(0, bucketEnd);
        String path = bucketEnd < 0 ? "" : rest.substring(bucketEnd + 1);

        if (bucket.isEmpty()) {
            throw reject(trimmed, "it has no bucket");
        }
        if (bucket.indexOf('@') >= 0) {
            throw reject(trimmed, "a bucket must not carry user info");
        }
        if (bucket.indexOf(':') >= 0) {
            throw reject(trimmed, "a bucket must not carry a port");
        }
        if (path.indexOf('?') >= 0) {
            throw reject(trimmed, "a location must not carry a query string");
        }
        if (path.indexOf('#') >= 0) {
            throw reject(trimmed, "a location must not carry a fragment");
        }

        // S3 bucket names are case-insensitive by construction, so folding them is safe. Keys are
        // case-sensitive and must not be folded.
        return new S3Location(bucket.toLowerCase(Locale.ROOT), normalizeSegments(trimmed, path));
    }

    private static List<String> normalizeSegments(String raw, String path) {
        List<String> normalized = new ArrayList<>();
        if (path == null) {
            return ImmutableList.of();
        }
        for (String rawSegment : path.split("/")) {
            // Decoded before the dot segments are recognized, not after: "%2E%2E" has to traverse up
            // exactly like ".." does. Deciding on the raw text first would leave it in the list as an
            // ordinary name, and a location that reads as a child of the table root here would resolve
            // outside it wherever the key is decoded later - which is the one question this class exists
            // to answer. An escape cannot produce a separator, because %2F was already refused.
            String segment = decodePercentEscapes(raw, rawSegment);
            // Empty segments cover both a trailing slash and a doubled separator; neither names a
            // level of the hierarchy.
            if (segment.isEmpty() || ".".equals(segment)) {
                continue;
            }
            if ("..".equals(segment)) {
                if (normalized.isEmpty()) {
                    throw reject(raw, "it traverses above the bucket root");
                }
                normalized.remove(normalized.size() - 1);
                continue;
            }
            normalized.add(segment);
        }
        return ImmutableList.copyOf(normalized);
    }

    /**
     * Decodes %XX escapes and nothing else.
     *
     * Deliberately not URLDecoder: it reads '+' as a space, which is form-encoding semantics. In an S3
     * key a '+' is just a plus, and folding it into a space would make two different objects compare
     * equal - so a partition under "s3://b/tbl+a" would pass as living under the root "s3://b/tbl a".
     *
     * A malformed escape is refused with our own exception type rather than URLDecoder's raw
     * IllegalArgumentException, which would slip past the fail-closed handlers that recognize
     * LakeFormationTableAccessException. A literal '%' is legal in an S3 key ("s3://b/100%done"), so
     * this is a real input, not a hypothetical.
     */
    private static String decodePercentEscapes(String raw, String segment) {
        if (segment.indexOf('%') < 0) {
            return segment;
        }
        byte[] in = segment.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(in.length);
        int i = 0;
        while (i < in.length) {
            if (in[i] != '%') {
                out.write(in[i]);
                i++;
                continue;
            }
            if (i + 2 >= in.length) {
                throw reject(raw, "it ends in a truncated percent escape");
            }
            int high = Character.digit((char) in[i + 1], 16);
            int low = Character.digit((char) in[i + 2], 16);
            if (high < 0 || low < 0) {
                throw reject(raw, "it has a malformed percent escape '"
                        + new String(in, i, 3, StandardCharsets.UTF_8) + "'");
            }
            out.write((high << 4) + low);
            i += 3;
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * True when this location is the given root or lives underneath it. Compared segment by segment,
     * so a sibling whose name merely shares a prefix is not a descendant.
     */
    public boolean isSameOrDescendantOf(S3Location root) {
        if (!bucket.equals(root.bucket)) {
            return false;
        }
        if (segments.size() < root.segments.size()) {
            return false;
        }
        for (int i = 0; i < root.segments.size(); i++) {
            if (!segments.get(i).equals(root.segments.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "s3://" + bucket + "/" + String.join("/", segments);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof S3Location)) {
            return false;
        }
        S3Location that = (S3Location) other;
        return bucket.equals(that.bucket) && segments.equals(that.segments);
    }

    @Override
    public int hashCode() {
        return 31 * bucket.hashCode() + segments.hashCode();
    }

    private static LakeFormationTableAccessException reject(String raw, String reason) {
        return new LakeFormationTableAccessException(
                "Cannot use location '" + raw + "' with Lake Formation because " + reason);
    }
}
