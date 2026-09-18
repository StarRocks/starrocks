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

package com.starrocks.credential;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.credential.azure.AzureCloudConfiguration;
import com.starrocks.fs.hdfs.WildcardURI;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Decides whether a user-supplied storage property map authenticates only with secrets spelled out in
 * the map itself. Anything that would make the FE or BE resolve credentials from the node it runs on
 * (instance profile, SDK default chain, web identity, managed identity, compute engine service account)
 * or chain through another principal (IAM role assumption, service account impersonation) is a
 * violation, as is a credential for a different cloud than the path scheme resolves to, because a
 * client that finds no properties of its own silently falls back to a node-owned default identity.
 * <p>
 * Schemes are matched case-sensitively because {@code HdfsFsManager} dispatches on lowercase literals:
 * an upper-case scheme misses every case there and lands in the universal filesystem, which builds its
 * configuration from the node's Hadoop files instead of from these properties.
 * <p>
 * Everything reached through a Hadoop client is out of scope, because a Hadoop client resolves what the
 * properties do not supply from the node's own {@code core-site.xml} and {@code hdfs-site.xml}. That
 * rules out HDFS entirely: its identity is decided by those files rather than by the statement, and
 * simple authentication carries no secret at all, so no HDFS property map can express an identity this
 * policy could guarantee.
 * <p>
 * It also rules out most of Azure. Only {@code wasb://} and {@code wasbs://} are served by the native
 * Azure SDK, and only while {@code azure_use_native_sdk} is on -- on the FE in {@code FileSystem} and on
 * the BE in {@code fs_azblob.cpp}. That client takes the account from the URI and the credential from
 * the {@code azure.blob.*} properties alone, so what it will use is decided entirely by the statement.
 * Every other Azure scheme, and these two with the flag off, go through Hadoop, where a key is installed
 * per account under a hard-coded public-cloud suffix, some credential kinds install no Hadoop key at
 * all, and the configuration differs between the listing path and {@code TableFunctionTableSink}. Which
 * key would end up installed is then not decidable from the properties, so those paths are refused.
 */
public final class ExplicitCredentialPolicy {

    // Schemes that may be targeted under this policy, each with the credential type it must carry.
    private static final ImmutableMap<String, CloudType> SCHEME_TO_CLOUD_TYPE =
            ImmutableMap.<String, CloudType>builder()
                    .put("s3", CloudType.AWS)
                    .put("s3a", CloudType.AWS)
                    .put("oss", CloudType.ALIYUN)
                    .put("cosn", CloudType.TENCENT)
                    .put("gs", CloudType.GCP)
                    .put("wasb", CloudType.AZURE)
                    .put("wasbs", CloudType.AZURE)
                    .build();

    // Hadoop configuration namespaces. No supported scheme needs a property from one, while a property
    // here can name a credential provider, remap a filesystem implementation, load configuration files
    // or jars from the node's disk, or point at a secret stored on the node, so the whole set is
    // refused rather than filtered.
    private static final ImmutableSet<String> HADOOP_NAMESPACE_PREFIXES = ImmutableSet.of(
            "fs.", "hadoop.", "dfs.", "ipc.", "io.", "viewfs.", "yarn.", "mapreduce.", "mapred.");

    private static final String REDACTED = "<redacted>";

    private ExplicitCredentialPolicy() {
    }

    /**
     * @param pathList   one or more comma-separated URIs, as accepted by FILES()
     * @param properties the user-supplied storage properties
     * @return the first violation found, or empty when only explicit credentials are used
     */
    public static Optional<String> check(String pathList, Map<String, String> properties) {
        for (String key : properties.keySet()) {
            if (isHadoopProperty(key)) {
                return Optional.of("Hadoop property '" + key + "' is not allowed; it is resolved against the "
                        + "node's own configuration rather than the statement");
            }
        }

        List<String> paths = Splitter.on(",").trimResults().omitEmptyStrings()
                .splitToList(Strings.nullToEmpty(pathList));
        if (paths.isEmpty()) {
            return Optional.of("path is empty");
        }
        CloudType requiredType = null;
        List<String> schemes = new ArrayList<>();
        for (String path : paths) {
            String scheme = schemeOf(path);
            CloudType type = scheme == null ? null : SCHEME_TO_CLOUD_TYPE.get(scheme);
            if (type == null) {
                return Optional.of("path scheme '" + scheme + "' is not supported with explicit credentials: "
                        + redactPath(path));
            }
            if (requiredType != null && requiredType != type) {
                return Optional.of("paths span different storage types: " + redactPathList(pathList));
            }
            requiredType = type;
            schemes.add(scheme);
        }

        if (requiredType == CloudType.AZURE && !Config.azure_use_native_sdk) {
            // With the flag off these paths are opened by the Hadoop client, whose credential is not
            // decidable from the properties alone.
            return Optional.of("an Azure path needs azure_use_native_sdk to be enabled, so that the "
                    + "credential comes from these properties rather than from the node's Hadoop "
                    + "configuration");
        }

        // Built from the properties alone, which is all that every path opening the target is given: the
        // listing path may hand the provider extra context, but TableFunctionTableSink builds an
        // unload's configuration from the properties only.
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>(properties));
        if (cloudConfiguration == null || cloudConfiguration.getCloudType() == CloudType.DEFAULT
                || cloudConfiguration.getCloudCredential() == null) {
            return Optional.of("no explicit storage credentials found in properties");
        }
        if (cloudConfiguration.getCloudType() != requiredType) {
            return Optional.of(String.format("credentials are for %s but path scheme requires %s credentials",
                    cloudConfiguration.getCloudType(), requiredType));
        }
        if (cloudConfiguration instanceof AzureCloudConfiguration azureConfiguration) {
            // Every Azure credential reports CloudType.AZURE, but the Blob client reads only the Blob
            // properties and falls back to DefaultAzureCredential when it finds none of them.
            for (String scheme : schemes) {
                if (!azureConfiguration.matchesScheme(scheme)) {
                    return Optional.of("credentials are for a different Azure storage service than path scheme '"
                            + scheme + "' resolves to");
                }
            }
        }
        return cloudConfiguration.getCloudCredential().delegatedIdentity()
                .map(identity -> "credentials rely on " + identity);
    }

    /**
     * A path reduced to what a rejection needs in order to name it: the scheme and authority. Object
     * names live in the rest of the path and a token can live in the query string, so neither is kept,
     * and a user-info that could hide a password is masked. Rejections are logged, and a rejected URI is
     * entirely user-controlled.
     */
    public static String redactPath(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return REDACTED;
        }
        try {
            URI uri = new WildcardURI(path).getUri();
            String scheme = uri.getScheme();
            String authority = uri.getRawAuthority();
            if (scheme == null || authority == null) {
                return REDACTED;
            }
            return scheme + "://" + maskUserInfo(authority) + "/...";
        } catch (StarRocksException e) {
            return REDACTED;
        }
    }

    /** {@link #redactPath} over a comma-separated list, as FILES() accepts one. */
    public static String redactPathList(String pathList) {
        List<String> paths = Splitter.on(",").trimResults().omitEmptyStrings()
                .splitToList(Strings.nullToEmpty(pathList));
        if (paths.isEmpty()) {
            return REDACTED;
        }
        return paths.stream().map(ExplicitCredentialPolicy::redactPath).collect(Collectors.joining(", "));
    }

    // An authority may carry user:password@host, percent-encoded or not. The container of a wasb URI
    // sits in the same place, but it is not needed to name the target in a message, so any user-info is
    // replaced whole rather than parsed for its password half.
    private static String maskUserInfo(String authority) {
        int at = authority.lastIndexOf('@');
        return at < 0 ? authority : "******" + authority.substring(at);
    }

    private static boolean isHadoopProperty(String key) {
        String lowerCaseKey = key.toLowerCase();
        return HADOOP_NAMESPACE_PREFIXES.stream().anyMatch(lowerCaseKey::startsWith);
    }

    // Returned verbatim: HdfsFsManager switches on lowercase scheme literals, so an upper-case scheme
    // is a different, unsupported path rather than the same one spelled differently.
    private static String schemeOf(String path) {
        try {
            return new WildcardURI(path).getUri().getScheme();
        } catch (StarRocksException e) {
            return null;
        }
    }
}
