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

package com.starrocks.connector.delta;

import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import io.delta.storage.S3SingleDriverLogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AWSBadRequestException;
import org.apache.hadoop.fs.s3a.AWSUnsupportedFeatureException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.Listing;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.S3ListRequest;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_LIST_STATUS;

/**
 * Delta log listing that starts the S3 listing at the requested file instead of listing all of _delta_log.
 *
 * <p>Delta's own fast path ({@code delta.enableFastS3AListFrom}) sends no delimiter, so it also returns every
 * key in subdirectories such as {@code _staged_commits/}. Kernel classifies log files by file name only, so it
 * would read unpublished catalog-managed commits ({@code <version>.<uuid>.json}) as published ones. This
 * listing sends the "/" delimiter, which returns subdirectories as single entries like a full listing does.
 */
public class DeltaLakeS3LogStore extends S3SingleDriverLogStore {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeS3LogStore.class);
    private static final List<String> SCHEMES = List.of("s3", "s3a", "s3n");
    private static final String DELIMITER = "/";

    public DeltaLakeS3LogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    public static void registerAsDefault(Configuration hadoopConf) {
        for (String scheme : SCHEMES) {
            hadoopConf.setIfUnset("io.delta.kernel.logStore." + scheme + ".impl",
                    DeltaLakeS3LogStore.class.getName());
        }
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.listFrom")) {
            FileSystem fs = path.getFileSystem(hadoopConf);
            Path resolvedPath = fs.makeQualified(path);

            final var enabled = Config.enable_delta_lake_s3_start_after_listing;
            if (!enabled || !(fs instanceof S3AFileSystem s3afs) || usesListV1(s3afs)) {
                return fullListing(fs, resolvedPath);
            }

            FileStatus[] statuses;
            try {
                // Optimized path. Only get files from the start file on and do not list everything.
                statuses = listFromStartFile(s3afs, resolvedPath);
            } catch (IOException | RuntimeException e) {
                if (!fullListingMayRecover(e)) {
                    throw e;
                }
                // Some S3-compatible stores reject start-after; the listing internals are unstable Hadoop APIs.
                LOG.warn("S3 start-after listing of {} failed, falling back to a full listing", resolvedPath, e);
                Tracers.count(EXTERNAL, "DeltaLake.listFrom.s3Fallback", 1);
                return fullListing(fs, resolvedPath);
            }
            Tracers.count(EXTERNAL, "DeltaLake.listFrom.s3Files", statuses.length);
            // S3 lists a missing directory as empty. Only check for it then, instead of before every listing.
            if (statuses.length == 0 && !fs.exists(resolvedPath.getParent())) {
                throw logDirNotFound(resolvedPath);
            }
            return sortedFrom(statuses, resolvedPath.getName());
        }
    }

    /**
     * The full listing of Delta's stock S3SingleDriverLogStore, without its delta.enableFastS3AListFrom path,
     * which would list subdirectories recursively.
     */
    private static Iterator<FileStatus> fullListing(FileSystem fs, Path resolvedPath) throws IOException {
        Path parentPath = resolvedPath.getParent();
        if (!fs.exists(parentPath)) {
            throw logDirNotFound(resolvedPath);
        }
        return sortedFrom(fs.listStatus(parentPath), resolvedPath.getName());
    }

    private static FileStatus[] listFromStartFile(S3AFileSystem s3afs, Path resolvedPath) throws IOException {
        Path parentPath = resolvedPath.getParent();
        String prefix = s3afs.maybeAddTrailingSlash(s3afs.pathToKey(parentPath));
        int maxKeys = S3AUtils.intOption(s3afs.getConf(), Constants.MAX_PAGING_KEYS,
                Constants.DEFAULT_MAX_PAGING_KEYS, 1);
        // Same request S3AFileSystem#listStatus sends, plus start-after.
        try (AuditSpan span = s3afs.createSpan(OP_LIST_STATUS, parentPath.toString(), null)) {
            ListObjectsV2Request request = s3afs.getRequestFactory()
                    .newListObjectsV2RequestBuilder(prefix, DELIMITER, maxKeys)
                    .startAfter(keyBefore(prefix + resolvedPath.getName()))
                    .build();
            return S3AUtils.iteratorToStatuses(s3afs.getListing().createFileStatusListingIterator(parentPath,
                    S3ListRequest.v2(request), S3AUtils.ACCEPT_ALL,
                    new Listing.AcceptAllButSelfAndS3nDirs(parentPath), span));
        }
    }

    /**
     * The key right before {@code key}, so that the exclusive start-after includes {@code key} itself.
     * Kernel lists from ASCII names such as "00000000000000712884."; for any other last character this
     * starts at the directory's first key, which the name filter then trims.
     */
    static String keyBefore(String key) {
        char last = key.charAt(key.length() - 1);
        if (last == 0 || last > 0x7F) {
            return null;
        }
        return key.substring(0, key.length() - 1) + (char) (last - 1);
    }

    /** start-after only exists in ListObjectsV2; stores configured for V1 keep the full listing. */
    private static boolean usesListV1(S3AFileSystem s3afs) {
        return s3afs.getConf().getInt(Constants.LIST_VERSION, Constants.DEFAULT_LIST_VERSION) == 1;
    }

    /**
     * Whether the full listing can succeed where the start-after listing failed with {@code e}: the store
     * rejected the request (S3A maps 400 and 409 to AWSBadRequestException, 405, 415 and 501 to
     * AWSUnsupportedFeatureException), or the listing internals failed. Missing directories, denied access,
     * throttling, server and network errors (after S3A's own retries) and cancellation would repeat.
     */
    private static boolean fullListingMayRecover(Exception e) {
        if (e instanceof AWSBadRequestException || e instanceof AWSUnsupportedFeatureException) {
            return true;
        }
        return e instanceof RuntimeException && !Thread.currentThread().isInterrupted();
    }

    /** LogStore's lexical ordering and inclusive lower bound, including checkpoint parts and checksum files. */
    private static Iterator<FileStatus> sortedFrom(FileStatus[] statuses, String startName) {
        return Arrays.stream(statuses)
                .filter(status -> status.getPath().getName().compareTo(startName) >= 0)
                .sorted(Comparator.comparing(status -> status.getPath().getName()))
                .iterator();
    }

    private static FileNotFoundException logDirNotFound(Path resolvedPath) {
        return new FileNotFoundException(String.format("No such file or directory: %s", resolvedPath.getParent()));
    }
}
