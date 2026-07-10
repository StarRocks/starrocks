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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.share.credential.AwsCredentialUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.credential.aws.AwsCloudCredential;
import com.starrocks.fs.FileSystem;
import com.starrocks.fs.hdfs.HdfsFileSystemWrap;
import com.starrocks.fs.hdfs.WildcardURI;
import com.starrocks.thrift.THdfsProperties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A native S3 (and S3-compatible: s3a/s3n/oss/cosn/ks3/obs/tos) {@link FileSystem} implementation
 * used to accelerate glob listing for the FILES() table function.
 *
 * <p>The Hadoop {@code S3AFileSystem.globStatus} path resolves a wildcard pattern by listing the
 * parent "directory" of the first wildcard path component and filtering client-side. Because
 * Hadoop's {@code Globber} splits the pattern on '/', the literal head inside a wildcard component
 * (e.g. {@code 2026-06-24-13} in {@code sn-fstore/2026-06-24-13*}) is never pushed down as an S3
 * {@code ListObjectsV2} prefix. For a bucket where the parent prefix holds millions of objects this
 * enumerates the whole prefix even when only a handful match.
 *
 * <p>This implementation instead pushes the longest literal prefix of each wildcard component down
 * into {@code ListObjectsV2(prefix=...)} so S3 returns only the matching keys, then applies a
 * {@link GlobFilter} client-side to preserve Hadoop glob semantics (notably that {@code *} does not
 * cross '/'). Only glob listing is overridden; {@link #getHdfsProperties} is delegated to
 * {@link HdfsFileSystemWrap} so the backend read path is unchanged.
 */
public class S3FileSystem implements FileSystem {
    private static final Logger LOG = LogManager.getLogger(S3FileSystem.class);

    private static final String SEPARATOR = "/";
    // Glob metacharacters recognized by Hadoop GlobPattern. Their presence marks a component as a
    // wildcard; the literal prefix of a component is everything before the first of these.
    private static final String GLOB_METACHARACTERS = "*?[{\\";

    // S3-compatible schemes that Hadoop routes through S3AFileSystem (see HdfsFsManager.getFileSystem).
    private static final Set<String> SUPPORTED_SCHEMES =
            ImmutableSet.of("s3", "s3a", "s3n", "oss", "cosn", "ks3", "obs", "tos");

    private final Map<String, String> properties;
    // Reused for getHdfsProperties() and for the fallback glob path (exact paths / brace patterns).
    private final HdfsFileSystemWrap fallback;

    public S3FileSystem(Map<String, String> properties) {
        this.properties = properties;
        this.fallback = new HdfsFileSystemWrap(properties);
    }

    /**
     * Whether the native S3 glob path can serve the given scheme.
     */
    public static boolean isSupportedScheme(String scheme) {
        return scheme != null && SUPPORTED_SCHEMES.contains(scheme.toLowerCase());
    }

    /**
     * Build the cloud configuration and return it only when it is an AWS-style configuration. Returns
     * null when the properties do not resolve to AWS credentials (e.g. legacy {@code fs.oss.*} keys ->
     * AliyunCloudConfiguration) or when building fails, in which case the caller falls back to the
     * Hadoop path. The returned object is reused to build the client, so the map is parsed only once.
     */
    protected AwsCloudConfiguration tryGetAwsCloudConfiguration() {
        try {
            CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
            return cc instanceof AwsCloudConfiguration ? (AwsCloudConfiguration) cc : null;
        } catch (Exception e) {
            LOG.warn("Failed to build cloud configuration for native S3 glob, will fall back to hadoop path", e);
            return null;
        }
    }

    @Override
    public List<FileStatus> globList(String path, boolean skipDir) throws StarRocksException {
        WildcardURI uri = new WildcardURI(path);
        String scheme = uri.getUri().getScheme();
        String bucket = uri.getAuthority();
        // uri.getPath() is "" for a bucket-root path (s3://bucket) and null for a malformed opaque
        // URI (s3:foo); normalize both to "" so they fall back to the Hadoop path instead of NPEing.
        String keyPattern = stripLeadingSlash(Strings.nullToEmpty(uri.getPath()));

        // Fall back to the Hadoop globStatus path for cases the native prefix pushdown does not
        // handle:
        //   - brace expansion "{a,b}" (Hadoop expands it via GlobExpander; GlobFilter alone cannot),
        //   - exact paths with no wildcard (Hadoop getFileStatus is a single efficient HEAD).
        if (keyPattern.indexOf('{') >= 0 || !containsGlobWildcard(keyPattern)) {
            return fallbackGlobList(path, skipDir);
        }

        // Resolve the cloud configuration once here (after the cheap pattern gate) and reuse it to
        // build the client. Non-AWS configurations fall back to the Hadoop path.
        AwsCloudConfiguration awsCloudConfiguration = tryGetAwsCloudConfiguration();
        if (awsCloudConfiguration == null) {
            return fallbackGlobList(path, skipDir);
        }

        long startTime = System.currentTimeMillis();
        try (S3Client s3Client = createS3Client(awsCloudConfiguration)) {
            List<FileStatus> result = nativeGlobList(s3Client, scheme, bucket, keyPattern);
            List<FileStatus> filtered = skipDir
                    ? result.stream().filter(f -> !f.isDirectory()).collect(Collectors.toList())
                    : result;
            // S3 returns CommonPrefixes (dirs) and Contents (files) as separate groups; sort by path
            // so the result order matches the Hadoop globStatus path (Globber does Arrays.sort) and is
            // deterministic.
            filtered.sort(null);
            if (LOG.isDebugEnabled()) {
                LOG.debug("S3 native globList done, path={}, matched={}, costMs={}",
                        path, filtered.size(), System.currentTimeMillis() - startTime);
            }
            return filtered;
        } catch (Exception e) {
            // Availability over optimization: any unexpected failure on the native path (e.g. an
            // auth/region edge case the Hadoop path handles differently) falls back to globStatus so
            // the query still succeeds. If the fallback also fails, its exception propagates.
            LOG.warn("S3 native globList failed, falling back to hadoop globStatus. path: {}", path, e);
            return fallbackGlobList(path, skipDir);
        }
    }

    @Override
    public THdfsProperties getHdfsProperties(String path) throws StarRocksException {
        // BE read path must stay identical to the Hadoop-based implementation.
        return fallback.getHdfsProperties(path);
    }

    // Delegates to the Hadoop globStatus path for patterns the native path does not handle.
    protected List<FileStatus> fallbackGlobList(String path, boolean skipDir) throws StarRocksException {
        return fallback.globList(path, skipDir);
    }

    /**
     * Walk the pattern components the same way Hadoop's Globber does, but at each component that
     * requires a listing, push the component's literal head into the S3 ListObjectsV2 prefix.
     */
    private List<FileStatus> nativeGlobList(S3Client s3Client, String scheme, String bucket, String keyPattern)
            throws Exception {
        List<String> components = Splitter.on(SEPARATOR).omitEmptyStrings().splitToList(keyPattern);

        // Candidate directory keys accumulated so far ("" == bucket root). Intermediate candidates
        // are always directories; only terminal matches may be files.
        List<String> candidateKeys = Lists.newArrayList("");
        List<FileStatus> terminalResults = Lists.newArrayList();

        for (int idx = 0; idx < components.size(); ++idx) {
            String component = components.get(idx);
            boolean isLast = idx == components.size() - 1;
            GlobFilter globFilter = new GlobFilter(component);

            if (candidateKeys.isEmpty()) {
                break;
            }

            if (!isLast && !globFilter.hasPattern()) {
                // Non-terminal literal component: assume it exists and just extend the prefix,
                // mirroring the Globber optimization. A miss surfaces when a later listing is empty.
                candidateKeys = candidateKeys.stream()
                        .map(k -> k.isEmpty() ? component : k + SEPARATOR + component)
                        .collect(Collectors.toList());
                continue;
            }

            String literalHead = literalPrefixOf(component);
            List<String> nextKeys = Lists.newArrayList();
            for (String parentKey : candidateKeys) {
                for (FileStatus child : listWithPrefix(s3Client, scheme, bucket, parentKey, literalHead)) {
                    // Only directories can be recursed into for non-terminal components.
                    if (!isLast && !child.isDirectory()) {
                        continue;
                    }
                    // GlobFilter matches on the last path segment (getName()).
                    if (!globFilter.accept(child.getPath())) {
                        continue;
                    }
                    if (isLast) {
                        terminalResults.add(child);
                    } else {
                        nextKeys.add(keyOf(child.getPath()));
                    }
                }
            }
            candidateKeys = nextKeys;
        }

        return terminalResults;
    }

    /**
     * List entries directly under {@code parentKey} whose next path segment starts with
     * {@code literalHead}, using a single S3 ListObjectsV2 with delimiter '/'. CommonPrefixes become
     * directories, objects become files.
     */
    protected List<FileStatus> listWithPrefix(S3Client s3Client, String scheme, String bucket,
                                              String parentKey, String literalHead) {
        ListObjectsV2Request request = buildListObjectsRequest(bucket, parentKey, literalHead);
        List<FileStatus> children = Lists.newArrayList();
        for (ListObjectsV2Response page : s3Client.listObjectsV2Paginator(request)) {
            collectPage(scheme, bucket, page, children);
        }
        return children;
    }

    // Build a ListObjectsV2 request scoped to entries directly under parentKey whose next segment
    // starts with literalHead. Delimiter '/' lists a single hierarchy level.
    static ListObjectsV2Request buildListObjectsRequest(String bucket, String parentKey, String literalHead) {
        String prefix = parentKey.isEmpty() ? literalHead : parentKey + SEPARATOR + literalHead;
        return ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .delimiter(SEPARATOR)
                .build();
    }

    // Translate one ListObjectsV2 response page into FileStatus entries: CommonPrefixes become
    // directories, objects become files, and "directory placeholder" objects (keys ending with '/')
    // are dropped.
    static void collectPage(String scheme, String bucket, ListObjectsV2Response page, List<FileStatus> out) {
        for (CommonPrefix commonPrefix : page.commonPrefixes()) {
            String dirKey = stripTrailingSlash(commonPrefix.prefix());
            out.add(new FileStatus(0, true, 1, 1, 0, buildPath(scheme, bucket, dirKey)));
        }
        for (S3Object object : page.contents()) {
            String key = object.key();
            if (key.endsWith(SEPARATOR)) {
                continue;
            }
            long size = object.size() == null ? 0L : object.size();
            long mtime = object.lastModified() == null ? 0L : object.lastModified().toEpochMilli();
            out.add(new FileStatus(size, false, 1, 1, mtime, buildPath(scheme, bucket, key)));
        }
    }

    protected S3Client createS3Client(AwsCloudConfiguration awsCloudConfiguration) {
        AwsCloudCredential awsCloudCredential = awsCloudConfiguration.getAwsCloudCredential();

        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(awsCloudCredential.generateAWSCredentialsProvider())
                .region(awsCloudCredential.tryToResolveRegion())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(awsCloudConfiguration.getEnablePathStyleAccess())
                        .build());
        String endpoint = awsCloudCredential.getEndpoint();
        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(AwsCredentialUtil.ensureSchemeInEndpoint(endpoint));
        }
        return builder.build();
    }

    // Longest literal prefix of a component: everything before the first unescaped glob
    // metacharacter, with escapes resolved. A backslash escapes the next character, so "\X" is a
    // literal X that is still part of the prefix (matching Hadoop GlobPattern). For example
    // "part-\*abc*.parquet" yields "part-*abc", which can be pushed down as the ListObjectsV2 prefix.
    private static String literalPrefixOf(String component) {
        StringBuilder head = new StringBuilder(component.length());
        for (int i = 0; i < component.length(); ++i) {
            char c = component.charAt(i);
            if (c == '\\') {
                if (i + 1 >= component.length()) {
                    break; // trailing backslash: stop, treat as end of the literal head
                }
                head.append(component.charAt(++i)); // the escaped char is a literal
                continue;
            }
            if (GLOB_METACHARACTERS.indexOf(c) >= 0) {
                break; // first unescaped wildcard
            }
            head.append(c);
        }
        return head.toString();
    }

    private static boolean containsGlobWildcard(String key) {
        for (int i = 0; i < key.length(); ++i) {
            char c = key.charAt(i);
            // A backslash escapes the next character, so "\X" is a literal X, not a wildcard
            // (matching Hadoop GlobPattern). Skip the escaped char so escaped-only patterns such as
            // "part-\*.parquet" are treated as exact paths and fall back to the efficient HEAD path.
            if (c == '\\') {
                i++;
                continue;
            }
            if (GLOB_METACHARACTERS.indexOf(c) >= 0) {
                return true;
            }
        }
        return false;
    }

    // Build a full "scheme://bucket/key" path without re-parsing the key as a URI, so object keys
    // containing characters like spaces or '%' are preserved verbatim.
    private static Path buildPath(String scheme, String bucket, String key) {
        return new Path(scheme, bucket, SEPARATOR + key);
    }

    private static String keyOf(Path path) {
        return stripLeadingSlash(path.toUri().getPath());
    }

    private static String stripLeadingSlash(String s) {
        return s.startsWith(SEPARATOR) ? s.substring(1) : s;
    }

    private static String stripTrailingSlash(String s) {
        return s.endsWith(SEPARATOR) ? s.substring(0, s.length() - 1) : s;
    }
}