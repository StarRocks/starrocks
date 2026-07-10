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

package com.starrocks.fs.s3;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.StarRocksException;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3FileSystemTest {

    // Properties that resolve to an AwsCloudConfiguration, so globList takes the native path
    // (the real client is never built because createS3Client is overridden in the fake).
    private static Map<String, String> awsProps() {
        Map<String, String> props = Maps.newHashMap();
        props.put("aws.s3.use_aws_sdk_default_behavior", "true");
        return props;
    }

    private static FileStatus file(String key) {
        return new FileStatus(10, false, 1, 1, 0, new Path("s3", "bucket", "/" + key));
    }

    private static FileStatus dir(String key) {
        return new FileStatus(0, true, 1, 1, 0, new Path("s3", "bucket", "/" + key));
    }

    private static List<String> sortedNames(List<FileStatus> files) {
        return files.stream().map(f -> f.getPath().getName()).sorted().collect(Collectors.toList());
    }

    /**
     * Intercepts the per-level S3 listing so tests can assert the pushed-down prefix and feed canned
     * children without contacting S3. Also records fallback delegation.
     */
    private static class FakeS3FileSystem extends S3FileSystem {
        // Each entry is the [parentKey, literalHead] pushed down for one listing call.
        final List<String[]> pushedDown = Lists.newArrayList();
        // canned[prefix] -> children returned for that prefix (prefix = parentKey + "/" + literalHead).
        final Map<String, List<FileStatus>> canned;
        boolean fellBack = false;
        boolean throwOnList = false;

        FakeS3FileSystem(Map<String, List<FileStatus>> canned) {
            super(awsProps());
            this.canned = canned;
        }

        @Override
        protected S3Client createS3Client(AwsCloudConfiguration awsCloudConfiguration) {
            return null; // unused: listWithPrefix is overridden
        }

        @Override
        protected List<FileStatus> listWithPrefix(S3Client s3Client, String scheme, String bucket,
                                                  String parentKey, String literalHead) {
            pushedDown.add(new String[] {parentKey, literalHead});
            if (throwOnList) {
                throw new RuntimeException("simulated S3 listing failure");
            }
            String prefix = parentKey.isEmpty() ? literalHead : parentKey + "/" + literalHead;
            return canned.getOrDefault(prefix, Lists.newArrayList());
        }

        @Override
        protected List<FileStatus> fallbackGlobList(String path, boolean skipDir) {
            fellBack = true;
            return Lists.newArrayList();
        }
    }

    @Test
    public void testPrefixPushdownAndGlobFilter() throws StarRocksException {
        Map<String, List<FileStatus>> canned = Maps.newHashMap();
        // Simulate what S3 returns for prefix "sn-fstore/2026-06-24-1".
        canned.put("sn-fstore/2026-06-24-1", Lists.newArrayList(
                file("sn-fstore/2026-06-24-13"),
                file("sn-fstore/2026-06-24-13x"),  // '?' matches exactly one char -> filtered out
                file("sn-fstore/2026-06-24-19")));
        FakeS3FileSystem fs = new FakeS3FileSystem(canned);

        List<FileStatus> res = fs.globList("s3://bucket/sn-fstore/2026-06-24-1?", true);

        // The literal head of the wildcard component is pushed down, not just the parent dir.
        assertEquals(1, fs.pushedDown.size());
        assertEquals("sn-fstore", fs.pushedDown.get(0)[0]);
        assertEquals("2026-06-24-1", fs.pushedDown.get(0)[1]);
        // GlobFilter is still applied client-side to honor '?' semantics.
        assertEquals(Lists.newArrayList("2026-06-24-13", "2026-06-24-19"), sortedNames(res));
    }

    @Test
    public void testMultiLevelWildcardPushesDownEachLevel() throws StarRocksException {
        Map<String, List<FileStatus>> canned = Maps.newHashMap();
        canned.put("logs/2026-", Lists.newArrayList(
                dir("logs/2026-06"),
                dir("logs/2026-07"),
                file("logs/2026-note.txt")));      // a file at an intermediate level must not recurse
        canned.put("logs/2026-06/13", Lists.newArrayList(
                file("logs/2026-06/13-a.parquet"),
                file("logs/2026-06/139.parquet")));
        canned.put("logs/2026-07/13", Lists.newArrayList(
                file("logs/2026-07/13-b.parquet")));
        FakeS3FileSystem fs = new FakeS3FileSystem(canned);

        List<FileStatus> res = fs.globList("s3://bucket/logs/2026-*/13*", true);

        // Level 1 pushes down "2026-" under "logs"; level 2 pushes down "13" under each matched dir.
        assertTrue(fs.pushedDown.stream()
                .anyMatch(p -> p[0].equals("logs") && p[1].equals("2026-")));
        assertTrue(fs.pushedDown.stream()
                .anyMatch(p -> p[0].equals("logs/2026-06") && p[1].equals("13")));
        assertTrue(fs.pushedDown.stream()
                .anyMatch(p -> p[0].equals("logs/2026-07") && p[1].equals("13")));
        assertEquals(Lists.newArrayList("13-a.parquet", "13-b.parquet", "139.parquet"), sortedNames(res));
    }

    @Test
    public void testSkipDirFlag() throws StarRocksException {
        Map<String, List<FileStatus>> canned = Maps.newHashMap();
        canned.put("data/part", Lists.newArrayList(
                file("data/part-0.parquet"),
                dir("data/part-subdir")));
        FakeS3FileSystem withDirs = new FakeS3FileSystem(canned);
        List<FileStatus> all = withDirs.globList("s3://bucket/data/part*", false);
        assertEquals(Lists.newArrayList("part-0.parquet", "part-subdir"), sortedNames(all));

        FakeS3FileSystem skip = new FakeS3FileSystem(canned);
        List<FileStatus> filesOnly = skip.globList("s3://bucket/data/part*", true);
        assertEquals(Lists.newArrayList("part-0.parquet"), sortedNames(filesOnly));
    }

    @Test
    public void testBracePatternFallsBack() throws StarRocksException {
        FakeS3FileSystem fs = new FakeS3FileSystem(Maps.newHashMap());
        fs.globList("s3://bucket/data/part-{a,b}.parquet", true);
        assertTrue(fs.fellBack);
        assertTrue(fs.pushedDown.isEmpty());
    }

    @Test
    public void testExactPathFallsBack() throws StarRocksException {
        FakeS3FileSystem fs = new FakeS3FileSystem(Maps.newHashMap());
        fs.globList("s3://bucket/data/part-0.parquet", true);
        assertTrue(fs.fellBack);
        assertTrue(fs.pushedDown.isEmpty());
    }

    @Test
    public void testEscapedWildcardFallsBack() throws StarRocksException {
        // "\*" is a literal '*', not a wildcard, so this is an exact path and must not take the
        // native listing path.
        FakeS3FileSystem fs = new FakeS3FileSystem(Maps.newHashMap());
        fs.globList("s3://bucket/data/part-\\*.parquet", true);
        assertTrue(fs.fellBack);
        assertTrue(fs.pushedDown.isEmpty());
    }

    @Test
    public void testNativeFailureFallsBackToHadoop() throws StarRocksException {
        FakeS3FileSystem fs = new FakeS3FileSystem(Maps.newHashMap());
        fs.throwOnList = true;
        fs.globList("s3://bucket/sn-fstore/2026-06-24-13*", true);
        // The native path was attempted, then it fell back to the Hadoop globStatus path.
        assertTrue(fs.pushedDown.size() >= 1);
        assertTrue(fs.fellBack);
    }

    @Test
    public void testBuildListObjectsRequestPushesNarrowPrefix() {
        ListObjectsV2Request request =
                S3FileSystem.buildListObjectsRequest("mybucket", "sn-fstore", "2026-06-24-13");
        assertEquals("mybucket", request.bucket());
        assertEquals("sn-fstore/2026-06-24-13", request.prefix());
        assertEquals("/", request.delimiter());

        // Root-level listing (empty parent) uses the literal head verbatim.
        ListObjectsV2Request rootRequest = S3FileSystem.buildListObjectsRequest("mybucket", "", "logs-");
        assertEquals("logs-", rootRequest.prefix());
    }

    @Test
    public void testCollectPageParsesResponse() {
        ListObjectsV2Response page = ListObjectsV2Response.builder()
                .contents(
                        S3Object.builder().key("sn-fstore/2026-06-24-13-a.parquet")
                                .size(5L).lastModified(Instant.ofEpochMilli(1000)).build(),
                        // directory placeholder object, must be skipped
                        S3Object.builder().key("sn-fstore/2026-06-24-13/").size(0L).build())
                .commonPrefixes(CommonPrefix.builder().prefix("sn-fstore/2026-06-24-13-dir/").build())
                .build();

        List<FileStatus> out = Lists.newArrayList();
        S3FileSystem.collectPage("s3", "mybucket", page, out);

        // File + common-prefix dir are returned; the "/"-suffixed placeholder object is dropped.
        assertEquals(Lists.newArrayList("2026-06-24-13-a.parquet", "2026-06-24-13-dir"), sortedNames(out));
        FileStatus dir = out.stream().filter(FileStatus::isDirectory).findFirst().orElseThrow();
        assertEquals("s3://mybucket/sn-fstore/2026-06-24-13-dir", dir.getPath().toString());
        FileStatus file = out.stream().filter(f -> !f.isDirectory()).findFirst().orElseThrow();
        assertEquals(5, file.getLen());
        assertEquals(1000, file.getModificationTime());
    }
}