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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.CredentialRefreshPolicy;
import org.apache.iceberg.Table;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Supplier;

import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN_EXPIRES_AT;

/**
 * The credentials a catalog vended for one table in its loadTable response, and when a running scan should
 * ask for newer ones.
 *
 * <p>These expire (about an hour for the common REST catalogs) and are fixed into the fragment when it is
 * deployed, so a scan that runs longer would fail on the storage side. The backend asks its coordinator for
 * a newer configuration once {@link #refreshAfterMs} passes, and the coordinator answers by loading the
 * table again, past every table cache.
 */
public record IcebergVendedCredentials(CloudConfiguration cloudConfiguration, long refreshAfterMs) {
    // Iceberg's S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, and the per-account ADLS SAS prefix.
    static final String S3_SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";
    static final String ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX = "adls.sas-token-expires-at-ms.";

    // When the catalog does not say: STS session tokens and the common REST catalogs default to one hour.
    static final long ASSUMED_LIFETIME_MS = 3600_000L;
    // ...counted from when the table was loaded. When that is unknown too (a cache entry replaced meanwhile), the
    // credentials may be as old as the REST table cache keeps an entry, CachingIcebergCatalog.REST_TABLE_CACHE_MAX_TTL_SEC
    // (50 minutes), so at least ten minutes of an hour remain; renew after five, from a load that is known to be fresh.
    static final long CACHED_UNKNOWN_EXPIRY_REFRESH_MS = 5 * 60_000L;

    /**
     * @param loadedAtMs when nativeTable was loaded from the catalog, if known; asked only for credentials that
     *                   carry no expiry of their own
     * @return the table's own vended credentials, or null if its FileIO carries none - then the scan uses the
     *         catalog's credentials, which do not expire under it
     */
    public static IcebergVendedCredentials of(Table nativeTable, long nowMs, Supplier<OptionalLong> loadedAtMs) {
        Map<String, String> properties = nativeTable.io().properties();
        CloudConfiguration configuration =
                CloudConfigurationFactory.buildCloudConfigurationForVendedCredentials(properties, nativeTable.location());
        if (configuration.getCloudType() == CloudType.DEFAULT) {
            return null;
        }
        return new IcebergVendedCredentials(configuration, refreshAfterMs(properties, nowMs, loadedAtMs));
    }

    /**
     * The credentials of a table loaded again, past every table cache, for a scan planned on {@code planned}.
     *
     * @param tableName for error messages only
     * @throws StarRocksConnectorException if the name now resolves to a different table (dropped and created
     *         again), the table moved to another location, or it no longer vends credentials
     */
    public static IcebergVendedCredentials renewedFrom(Table planned, Table reloaded, long nowMs, String tableName) {
        UUID plannedUuid = uuidOf(planned);
        UUID reloadedUuid = uuidOf(reloaded);
        // Credentials are scoped to a table's location; never hand a running scan those of another table.
        if (plannedUuid != null && reloadedUuid != null && !plannedUuid.equals(reloadedUuid)) {
            throw new StarRocksConnectorException(String.format(
                    "Iceberg table %s was replaced while the scan was running (uuid %s, now %s)", tableName,
                    plannedUuid, reloadedUuid));
        }
        // Catalogs scope credentials to the table's current location; files planned under another one would be
        // denied with credentials the old ones are still valid for.
        if (!Objects.equals(planned.location(), reloaded.location())) {
            throw new StarRocksConnectorException(String.format(
                    "Iceberg table %s moved from %s to %s while the scan was running", tableName,
                    planned.location(), reloaded.location()));
        }
        IcebergVendedCredentials renewed = of(reloaded, nowMs, () -> OptionalLong.of(nowMs));
        if (renewed == null) {
            throw new StarRocksConnectorException("Iceberg table " + tableName + " no longer vends credentials");
        }
        return renewed;
    }

    // Null when the metadata has none (tables written by very old writers), which then cannot be compared.
    private static UUID uuidOf(Table table) {
        try {
            return table.uuid();
        } catch (RuntimeException e) {
            return null;
        }
    }

    static long refreshAfterMs(Map<String, String> properties, long nowMs, Supplier<OptionalLong> loadedAtMs) {
        long expiresAtMs = Long.MAX_VALUE;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.equals(S3_SESSION_TOKEN_EXPIRES_AT_MS) || key.equals(GCS_ACCESS_TOKEN_EXPIRES_AT)
                    || key.startsWith(ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX)) {
                try {
                    expiresAtMs = Math.min(expiresAtMs, Long.parseLong(entry.getValue().trim()));
                } catch (NumberFormatException e) {
                    // Unreadable: treated like an absent one.
                }
            }
        }
        if (expiresAtMs == Long.MAX_VALUE) {
            OptionalLong loadedAt = loadedAtMs.get();
            if (loadedAt.isEmpty()) {
                return nowMs + CACHED_UNKNOWN_EXPIRY_REFRESH_MS;
            }
            long receivedAtMs = Math.min(loadedAt.getAsLong(), nowMs);
            return CredentialRefreshPolicy.refreshAfterMs(receivedAtMs + ASSUMED_LIFETIME_MS, receivedAtMs);
        }
        return CredentialRefreshPolicy.refreshAfterMs(expiresAtMs, nowMs);
    }
}
