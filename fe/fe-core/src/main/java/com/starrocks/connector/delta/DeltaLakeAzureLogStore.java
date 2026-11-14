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
import com.starrocks.common.util.concurrent.lock.BlockingCallValidator;
import io.delta.storage.AzureLogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsFileSystemAccess;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/** Delta log listing that pushes the start filename into Azure's paginated directory listing. */
public class DeltaLakeAzureLogStore extends AzureLogStore {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeAzureLogStore.class);
    private static final int HTTP_CONTINUE = 100;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final List<String> SCHEMES = List.of("abfs", "abfss");

    public DeltaLakeAzureLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    public static void registerAsDefault(Configuration hadoopConf) {
        for (String scheme : SCHEMES) {
            hadoopConf.setIfUnset("io.delta.kernel.logStore." + scheme + ".impl",
                    DeltaLakeAzureLogStore.class.getName());
        }
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.listFrom")) {
            FileSystem fs = path.getFileSystem(hadoopConf);

            final var enabled = Config.enable_delta_lake_azure_start_from_listing;
            if (!enabled || !(fs instanceof AzureBlobFileSystem azureFs) || usesBlobEndpoint(azureFs.getAbfsStore())) {
                return super.listFrom(path, hadoopConf);
            }

            BlockingCallValidator.validateNotUnderLock("remote-storage");
            List<FileStatus> listed;
            try {
                // Optimized path. Only get files from start path and do not list everything.
                listed = listFromStartFile(azureFs, path);
                if (listed.isEmpty()) {
                    // Either the directory is missing (flat-namespace DFS lists it as empty instead of 404) or
                    // Azure misread the start token. Fallback: The full listing reports the first and recovers from the second.
                    Tracers.count(EXTERNAL, "DeltaLake.listFrom.azureEmptyFallback", 1);
                } else if (!startsAtRequestedVersion(azureFs, path, listed)) {
                    LOG.warn("Azure startFrom listing of {} skipped the requested version, falling back to a full listing",
                            path);
                    Tracers.count(EXTERNAL, "DeltaLake.listFrom.azureFallback", 1);
                } else {
                    return listed.iterator();
                }
            } catch (IOException | RuntimeException e) {
                if (!fullListingMayRecover(e)) {
                    throw e;
                }
                // The startFrom overload is an unstable Hadoop API that builds its own continuation token.
                LOG.warn("Azure startFrom listing of {} failed, falling back to a full listing", path, e);
                Tracers.count(EXTERNAL, "DeltaLake.listFrom.azureFallback", 1);
            }
            // Non-optimized path.
            return super.listFrom(path, hadoopConf);
        }
    }

    // Hadoop sends startFrom listings to the DFS endpoint even for Blob-configured accounts, which may only reach Blob.
    private static boolean usesBlobEndpoint(AzureBlobFileSystemStore store) {
        return store.getAbfsConfiguration().getFsConfiguredServiceType() == AbfsServiceType.BLOB;
    }

    private static List<FileStatus> listFromStartFile(AzureBlobFileSystem azureFs, Path path) throws IOException {
        AzureBlobFileSystemStore store = azureFs.getAbfsStore();
        AbfsConfiguration abfsConf = store.getAbfsConfiguration();

        TracingContext tracingContext = new TracingContext(
                TracingContext.validateClientCorrelationID(abfsConf.getClientCorrelationId()),
                AbfsFileSystemAccess.getFileSystemId(azureFs), FSOperationType.LISTSTATUS, true,
                abfsConf.getTracingHeaderFormat(), null);
        FileStatus[] statuses;
        try {
            statuses = store.listStatus(azureFs.makeQualified(path.getParent()), path.getName(), tracingContext);
        } catch (AzureBlobFileSystemException e) {
            // Direct store calls bypass AzureBlobFileSystem's exception translation (e.g. 404 to
            // FileNotFoundException)
            AzureBlobFileSystem.checkException(path.getParent(), e);
            throw e;
        }
        Tracers.count(EXTERNAL, "DeltaLake.listFrom.azureFiles", statuses.length);
        // Keep LogStore's lexical ordering and inclusive lower bound, including checkpoint parts
        // and checksum files. Only entries at/after the bound have been fetched from Azure.
        return Arrays.stream(statuses)
                .filter(status -> status.getPath().getName().compareTo(path.getName()) >= 0)
                .sorted(Comparator.comparing(status -> status.getPath().getName()))
                .toList();
    }

    /**
     * Whether a non-empty listing plausibly starts where Kernel asked. Kernel lists from "<version>."
     * (FileNames.listingPrefix), also for versions that log cleanup removed (checkpoint search, time travel,
     * cleanup itself list from older versions or 0), so a later first entry is fine unless the requested
     * version's commit "<version>.json" still exists: then Azure resolved the client-built continuation token
     * past it. Costs one metadata request, and only on a mismatch. For a start that is not a version prefix
     * (ConflictChecker lists from a commit file), "<start>json" never exists, so the listing is accepted.
     */
    private static boolean startsAtRequestedVersion(FileSystem fs, Path path, List<FileStatus> listed)
            throws IOException {
        String start = path.getName();
        if (listed.get(0).getPath().getName().startsWith(start)) {
            return true;
        }
        return !fs.exists(new Path(path.getParent(), start + "json"));
    }

    /** Whether the full listing can succeed where the startFrom listing failed with {@code e}. */
    private static boolean fullListingMayRecover(Throwable e) {
        if (e instanceof FileNotFoundException || isInterrupted(e)) {
            // The full listing would report the same missing directory, and a cancelled query must stop.
            return false;
        }
        if (e instanceof AccessDeniedException || e instanceof SASTokenProviderException) {
            // The full listing uses the same credentials against the same DFS endpoint.
            return false;
        }
        if (e instanceof AbfsRestOperationException rest && isRetriedByAbfs(rest.getStatusCode())) {
            // ABFS has already exhausted its retries for this status against the same DFS endpoint; a full
            // listing would only start another retry budget there and add load.
            return false;
        }
        // Requests Azure rejected (e.g. 400 for a token it does not accept), driver and runtime errors.
        return true;
    }

    /** The statuses AbfsRetryPolicy.shouldRetry retries: network errors (-1), 408, 410, 429 and 5xx except 501/505. */
    private static boolean isRetriedByAbfs(int statusCode) {
        return statusCode < HTTP_CONTINUE
                || statusCode == HttpURLConnection.HTTP_CLIENT_TIMEOUT
                || statusCode == HttpURLConnection.HTTP_GONE
                || statusCode == HTTP_TOO_MANY_REQUESTS
                || (statusCode >= HttpURLConnection.HTTP_INTERNAL_ERROR
                        && statusCode != HttpURLConnection.HTTP_NOT_IMPLEMENTED
                        && statusCode != HttpURLConnection.HTTP_VERSION);
    }

    /**
     * ABFS retries an interrupted request and then wraps the InterruptedIOException in an
     * AbfsRestOperationException with status -1, but restores the thread's interrupt flag.
     * SocketTimeoutException extends InterruptedIOException but is a timeout, not a cancellation.
     */
    private static boolean isInterrupted(Throwable e) {
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException
                    || (cause instanceof InterruptedIOException && !(cause instanceof SocketTimeoutException))) {
                return true;
            }
        }
        return false;
    }
}
