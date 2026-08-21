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

package com.starrocks.storagevolume;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.ADLS2CredentialInfo;
import com.staros.proto.ADLS2FileStoreInfo;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AzBlobCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.GSFileStoreInfo;
import com.staros.proto.HDFSFileStoreInfo;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StorageVolume implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(StorageVolume.class);

    public enum StorageVolumeType {
        UNKNOWN,
        S3,
        HDFS,
        AZBLOB,
        ADLS2,
        GS
    }

    // Without id, the scenario like "create storage volume 'a', drop storage volume 'a', create storage volume 'a'"
    // can not be handled. They will be treated as the same storage volume.
    @SerializedName("i")
    private String id;

    @SerializedName("n")
    private String name;

    @SerializedName("s")
    private StorageVolumeType svt;

    @SerializedName("l")
    private List<String> locations;

    private CloudConfiguration cloudConfiguration;

    /**
     * Derived from {@link #cloudConfiguration} and never persisted: a volume that was stored with a
     * credential which can no longer be turned into a usable configuration stays readable, so that
     * it can be listed and dropped, but it must not be used to store data.
     */
    private boolean credentialUsable = true;

    @SerializedName("p")
    private Map<String, String> params;

    @SerializedName("c")
    private String comment;

    @SerializedName("e")
    private boolean enabled;

    /**
     * Each storage volume can have a virtual tablet bind to it.
     * It is used to create a virtual shard in starmgr, and the value's persistence is guaranteed in filestore of starmgr.
     * The value of `vTabletId` is not -1L if a virtual tablet needed.
     */
    @SerializedName("vt")
    private long vTabletId = -1L;

    /**
     * Same as `vTabletId`, but it is used to create a virtual shard group in starmgr, and the value's persistence
     * is also guaranteed in filestore of starmgr.
     */
    @SerializedName("vtg")
    private long vTabletGroupId = -1L;

    public static final String V_SHARD_ID = "v_shard_id";
    public static final String V_SHARD_GROUP_ID = "v_shard_group_id";

    public static String CREDENTIAL_MASK = "******";

    /**
     * The credential properties an Azure volume can be created with. Kept next to
     * {@link #getParamsFromFileStoreInfo}, which is the read-back side of the same round trip.
     */
    private static final List<String> AZURE_CREDENTIAL_PROPERTIES = ImmutableList.of(
            CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY,
            CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN,
            CloudConfigurationConstants.AZURE_BLOB_OAUTH2_USE_MANAGED_IDENTITY,
            CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID,
            CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET,
            CloudConfigurationConstants.AZURE_BLOB_OAUTH2_TENANT_ID,
            CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY,
            CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN,
            CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY,
            CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TENANT_ID,
            CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID,
            CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET,
            CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT,
            CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TOKEN_FILE);

    private String dumpMaskedParams(Map<String, String> params) {
        Gson gson = new Gson();
        Map<String, String> maskedParams = new HashMap<>(params);
        addMaskForCredential(maskedParams);
        return gson.toJson(maskedParams);
    }

    public StorageVolume(String id, String name, String svt, List<String> locations,
                         Map<String, String> params, boolean enabled, String comment) throws DdlException {
        this(id, name, svt, locations, params, enabled, comment, true);
    }

    /**
     * @param requireUsableCredential when false, a credential that can no longer be turned into
     *                                a usable configuration is tolerated instead of rejected.
     *                                Only the read-back path passes false: identity, locations
     *                                and virtual-tablet bookkeeping are still intact there, and
     *                                the operations that rely solely on those - listing, resolving
     *                                the name for a privilege check, dropping - have to keep
     *                                working, otherwise such a volume is stuck in the cluster
     *                                with no way to remove it.
     */
    private StorageVolume(String id, String name, String svt, List<String> locations,
                          Map<String, String> params, boolean enabled, String comment,
                          boolean requireUsableCredential) throws DdlException {
        this.id = id;
        this.name = name;
        this.svt = toStorageVolumeType(svt);
        this.locations = new ArrayList<>(locations);
        this.comment = comment;
        this.enabled = enabled;
        this.params = new HashMap<>(params);
        Map<String, String> configurationParams = new HashMap<>(params);
        preprocessAuthenticationIfNeeded(configurationParams);
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(configurationParams, true);
        this.credentialUsable = isValidCloudConfiguration(this.svt, this.cloudConfiguration);
        if (!this.credentialUsable) {
            if (requireUsableCredential) {
                throw new SemanticException("Storage params is not valid " + dumpMaskedParams(params));
            }
            LOG.warn("Storage volume '{}' has an unusable credential. It can still be listed and " +
                    "dropped, but not used: {}", name, dumpMaskedParams(params));
        }
        if (requireUsableCredential) {
            // Every volume that is about to be written goes through here, including the one
            // StorageVolumeMgr#replaceStorageVolume builds when a restore changes the volume type,
            // which reaches the file store without passing createFileStoreInfo.
            validateCredentialIsPersistable(this.cloudConfiguration, params);
        }
        validateStorageVolumeConstraints();
    }

    public StorageVolume(StorageVolume sv) throws DdlException {
        this.id = sv.id;
        this.name = sv.name;
        this.svt = sv.svt;
        this.locations = new ArrayList<>(sv.locations);
        this.comment = sv.comment;
        this.enabled = sv.enabled;
        this.vTabletId = sv.vTabletId;
        this.vTabletGroupId = sv.vTabletGroupId;
        // Same derivation as the main constructor: an AZBLOB credential takes its container from the
        // locations rather than from the properties, so rebuilding from the raw params alone would
        // fall through the cloud providers to the HDFS one and make a perfectly usable volume look
        // broken. Usability follows the rebuilt configuration for the same reason - a copied flag
        // would disagree with the configuration this copy actually holds.
        Map<String, String> configurationParams = new HashMap<>(sv.params);
        preprocessAuthenticationIfNeeded(configurationParams);
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(configurationParams, true);
        this.credentialUsable = isValidCloudConfiguration(this.svt, this.cloudConfiguration);
        this.params = new HashMap<>(sv.params);
        validateStorageVolumeConstraints();
    }

    private void validateStorageVolumeConstraints() throws DdlException {
        if (svt == StorageVolumeType.S3) {
            boolean enablePartitionedPrefix = Boolean.parseBoolean(
                    params.getOrDefault(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX, "false"));
            if (enablePartitionedPrefix) {
                for (String location : locations) {
                    URI uri = URI.create(location);
                    if (!uri.getPath().isEmpty() && !"/".equals(uri.getPath())) {
                        throw new DdlException(String.format(
                                "Storage volume '%s' has '%s'='true', the location '%s'" +
                                        " should not contain sub path after bucket name!",
                                this.name, CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX, location));
                    }
                }
            }
        }
    }

    public void setCloudConfiguration(Map<String, String> params) {
        Map<String, String> newParams = new HashMap<>(this.params);
        newParams.putAll(params);
        // Build from a preprocessed copy, exactly as the constructors do: an AZBLOB credential takes
        // its container from the locations, params deliberately never carries it, and building from
        // the raw map would fall through to the HDFS provider and refuse an ALTER of a perfectly
        // usable volume. The derived container stays out of what is stored.
        Map<String, String> configurationParams = new HashMap<>(newParams);
        preprocessAuthenticationIfNeeded(configurationParams);
        CloudConfiguration newConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(configurationParams, true);
        if (!isValidCloudConfiguration(svt, newConfiguration)) {
            throw new SemanticException("Storage params is not valid " + dumpMaskedParams(newParams));
        }
        validateCredentialIsPersistable(newConfiguration, newParams);
        // Assigned only once both checks pass, so a rejected ALTER leaves the volume untouched.
        this.cloudConfiguration = newConfiguration;
        this.params = newParams;
        // An ALTER is how a volume that was only readable - stored with a credential that can no
        // longer be used - gets repaired, so this derived flag has to follow the new configuration
        // rather than stay false until the next metadata reload. Both update paths set the type
        // before calling this, so the predicate already sees the final type.
        this.credentialUsable = isValidCloudConfiguration(svt, newConfiguration);
    }

    /**
     * False when the stored credential can no longer be turned into a usable configuration. Such a
     * volume is kept readable on purpose - see the tolerating constructor - so callers that are
     * about to store data through it have to reject it explicitly.
     */
    public boolean isCredentialUsable() {
        return credentialUsable;
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public long getVTabletId() {
        return vTabletId;
    }

    public void setVTabletId(long vTabletId) {
        this.vTabletId = vTabletId;
    }

    public long getVTabletGroupId() {
        return vTabletGroupId;
    }

    public void setVTabletGroupId(long vTabletGroupId) {
        this.vTabletGroupId = vTabletGroupId;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public void setLocations(List<String> locations) {
        this.locations = locations;
    }

    public List<String> getLocations() {
        return locations;
    }

    public void setType(String type) {
        this.svt = toStorageVolumeType(type);
    }

    public String getType() {
        return svt.toString();
    }

    public Map<String, String> getProperties() {
        return params;
    }

    public static StorageVolumeType toStorageVolumeType(String svt) {
        switch (svt.toLowerCase()) {
            case "s3":
                return StorageVolumeType.S3;
            case "hdfs":
                return StorageVolumeType.HDFS;
            case "azblob":
                return StorageVolumeType.AZBLOB;
            case "adls2":
                return StorageVolumeType.ADLS2;
            case "gs":
                return StorageVolumeType.GS;
            default:
                return StorageVolumeType.UNKNOWN;
        }
    }

    private static boolean isValidCloudConfiguration(StorageVolumeType svt, CloudConfiguration configuration) {
        switch (svt) {
            case S3:
                return configuration.getCloudType() == CloudType.AWS;
            case HDFS:
                return configuration.getCloudType() == CloudType.HDFS;
            case AZBLOB:
                return configuration.getCloudType() == CloudType.AZURE;
            case ADLS2:
                return configuration.getCloudType() == CloudType.AZURE;
            case GS:
                return configuration.getCloudType() == CloudType.GCP;
            default:
                return false;
        }
    }

    /**
     * Not every credential the factory accepts can be stored: a credential's {@code toFileStoreInfo}
     * only carries the fields the file store models, and anything else is dropped silently. A volume
     * created from such a credential is written successfully and can never be rebuilt into a usable
     * one afterwards, so reject it while the statement can still fail cleanly.
     *
     * <p>The first half of the check applies to every volume type and is deliberately generic: the
     * configuration is round-tripped through {@link FileStoreInfo} and has to come back as a usable
     * configuration of the same cloud, which also covers credential forms added later.
     *
     * <p>Staying the same cloud is not sufficient on Azure though, where the credential the factory
     * accepts is richer than what the file store models, so a volume can stay valid while quietly
     * authenticating as somebody else. The second half therefore requires, for Azure volumes, that
     * the credential properties given and the ones read back are the same set. The comparison runs
     * both ways on purpose: the file store records no OAuth2 flow, so the read-back path infers one,
     * and a property it infers is as much of a mode change as a property it drops.
     *
     * <p>The ADLS2 workload identity is refused by this comparison. Its token file has no field in
     * {@link ADLS2CredentialInfo}, so it lives only in the params of the FE that created the volume:
     * a read-back turns it into a managed identity, and the file store is also the only credential a
     * lake table's storage location carries, so nothing downstream ever sees the token. The English
     * and Chinese CREATE STORAGE VOLUME pages used to offer it and were corrected together with this
     * change. Supporting it needs a token file field in the file store, not a lenient check here.
     *
     * <p>The AWS web identity profile used to lose just as much, being stored as an assume role or a
     * default credential, but #77638 has since given it a credential case of its own, so it now
     * round-trips and needs nothing from this check. What stays lossy on S3 is the session token,
     * refused above.
     *
     * <p>The restored parameters go through the same {@link #preprocessAuthenticationIfNeeded}
     * derivation as the incoming ones, otherwise fields derived from the locations rather than the
     * properties - the AZBLOB container - would look lost and reject perfectly storable credentials.
     */
    private void validateCredentialIsPersistable(CloudConfiguration configuration, Map<String, String> params) {
        Map<String, String> restored = getParamsFromFileStoreInfo(configuration.toFileStoreInfo());
        preprocessAuthenticationIfNeeded(restored);
        CloudConfiguration restoredConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(restored, true);
        if (!isValidCloudConfiguration(svt, restoredConfiguration)) {
            Map<String, String> maskedParams = new HashMap<>(params);
            addMaskForCredential(maskedParams);
            throw new SemanticException(String.format(
                    "Storage params contain a credential that cannot be stored for a %s storage volume: %s",
                    svt, new Gson().toJson(maskedParams)));
        }
        if (svt == StorageVolumeType.S3
                && isCredentialPropertySet(params, CloudConfigurationConstants.AWS_S3_SESSION_TOKEN)) {
            // AwsSimpleCredentialInfo carries the access key and its secret and nothing else - the
            // credential's own toFileStoreInfo says as much with a TODO - so the session token is
            // dropped and what reads back is a pair of temporary keys that can no longer
            // authenticate. Unlike the ADLS2 workload identity this form is not documented for
            // storage volumes, and temporary keys would expire during the volume's life anyway, so
            // refuse it rather than warn.
            throw new SemanticException(String.format(
                    "Storage params contain a credential that cannot be stored for a %s storage volume, " +
                            "storing it would drop %s", svt, CloudConfigurationConstants.AWS_S3_SESSION_TOKEN));
        }
        if (svt != StorageVolumeType.AZBLOB && svt != StorageVolumeType.ADLS2) {
            return;
        }
        List<String> changedProperties = AZURE_CREDENTIAL_PROPERTIES.stream()
                .filter(key -> isCredentialPropertySet(params, key) != isCredentialPropertySet(restored, key))
                .sorted()
                .collect(Collectors.toList());
        if (!changedProperties.isEmpty()) {
            // Names only: the values are the credential we are refusing to store.
            throw new SemanticException(String.format(
                    "Storage params contain a credential that cannot be stored for a %s storage volume, " +
                            "storing it would change %s", svt, String.join(", ", changedProperties)));
        }
    }

    /**
     * A property explicitly turned off says as little about the credential as an absent one, and the
     * read-back path only writes the flags that are on, so treat both the same way. Without this an
     * {@code oauth2_use_managed_identity = false} spelled out next to a shared key would look like a
     * property the file store had dropped.
     */
    private static boolean isCredentialPropertySet(Map<String, String> params, String key) {
        String value = params.get(key);
        return !Strings.isNullOrEmpty(value) && !value.equalsIgnoreCase("false");
    }

    public static void addMaskForCredential(Map<String, String> params) {
        params.computeIfPresent(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AWS_S3_SECRET_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID,
                (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY, (key, value) -> CREDENTIAL_MASK);
    }

    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(name,
                svt.name(),
                String.valueOf(GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                        .getDefaultStorageVolumeId().equals(id)),
                Joiner.on(", ").join(locations),
                dumpMaskedParams(params),
                String.valueOf(enabled),
                String.valueOf(comment)));
    }

    public static FileStoreInfo createFileStoreInfo(String name, String svt,
                                                    List<String> locations, Map<String, String> params,
                                                    boolean enabled, String comment) throws DdlException {
        StorageVolume sv = new StorageVolume("", name, svt, locations, params, enabled, comment);
        return sv.toFileStoreInfo();
    }

    public FileStoreInfo toFileStoreInfo() {
        Map<String, String> properties = new HashMap<>();
        if (vTabletId != -1L) {
            properties.put(V_SHARD_ID, String.valueOf(vTabletId));
        }
        if (vTabletGroupId != -1L) {
            properties.put(V_SHARD_GROUP_ID, String.valueOf(vTabletGroupId));
        }
        // What the configuration serialises to has to be a file store of this volume's own type.
        // For a volume tolerated on read-back it is not: the factory chain falls through the cloud
        // providers to the HDFS one, which accepts anything, so an AZBLOB volume whose credential
        // cannot be rebuilt serialises to an HDFS file store carrying the azure properties as plain
        // Hadoop configuration. Writing that back would silently change the volume's type in the
        // file store rather than fail. The callers that write a volume back check
        // isCredentialUsable() first, so reaching this means one was added without that check.
        FileStoreInfo credentialInfo = cloudConfiguration.toFileStoreInfo();
        if (credentialInfo == null || !credentialInfo.getFsType().name().equals(svt.name())) {
            throw new IllegalStateException(String.format(
                    "Storage volume '%s' of type %s has a credential that serialises to %s, " +
                            "it must not be written back", name, svt,
                    credentialInfo == null ? "nothing" : credentialInfo.getFsType()));
        }
        FileStoreInfo.Builder builder = credentialInfo.toBuilder();
        builder.setFsKey(id)
                .setFsName(this.name)
                .setComment(this.comment)
                .setEnabled(this.enabled)
                .addAllLocations(locations)
                .putAllProperties(properties);
        return builder.build();
    }

    public static StorageVolume fromFileStoreInfo(FileStoreInfo fsInfo) throws DdlException {
        String svt = fsInfo.getFsType().toString();
        Map<String, String> params = getParamsFromFileStoreInfo(fsInfo);
        StorageVolume storageVolume = new StorageVolume(fsInfo.getFsKey(), fsInfo.getFsName(), svt,
                fsInfo.getLocationsList(), params, fsInfo.getEnabled(), fsInfo.getComment(), false);

        Map<String, String> propertiesMap = fsInfo.getPropertiesMap();
        if (propertiesMap.containsKey(V_SHARD_ID)) {
            String vTabletId = propertiesMap.get(V_SHARD_ID);
            try {
                storageVolume.setVTabletId(StringUtils.isEmpty(vTabletId) ? -1L : Long.parseLong(vTabletId));
            } catch (NumberFormatException e) {
                LOG.error("Failed to parse vTabletId from properties, vTabletId: {}", vTabletId, e);
            }
        }
        if (propertiesMap.containsKey(V_SHARD_GROUP_ID)) {
            if (storageVolume.getVTabletId() == -1L) {
                LOG.warn("vTabletId is not set for storage volume: {}, skip parsing and setting vTabletGroupId",
                        storageVolume.getName());
                return storageVolume;
            }
            String vTabletGroupId = propertiesMap.get(V_SHARD_GROUP_ID);
            try {
                storageVolume.setVTabletGroupId(
                        StringUtils.isEmpty(vTabletGroupId) ? -1L : Long.parseLong(vTabletGroupId));
            } catch (NumberFormatException e) {
                // ignore processing further if the vTabletGroupId is not set, 
                // the previous vTabletId will be cleared by StarMgrMetaSyncer
                LOG.error("Failed to parse vTabletGroupId from properties, vTabletGroupId: {}", vTabletGroupId, e);
            }
        }

        return storageVolume;
    }

    public static Map<String, String> getParamsFromFileStoreInfo(FileStoreInfo fsInfo) {
        Map<String, String> params = new HashMap<>();
        switch (fsInfo.getFsType()) {
            case S3:
                S3FileStoreInfo s3FileStoreInfo = fsInfo.getS3FsInfo();
                params.put(CloudConfigurationConstants.AWS_S3_REGION, s3FileStoreInfo.getRegion());
                params.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, s3FileStoreInfo.getEndpoint());
                if (s3FileStoreInfo.getPartitionedPrefixEnabled()) {
                    // Don't show the parameters if not enabled.
                    params.put(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX,
                            Boolean.toString(true));
                    params.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX,
                            Integer.toString(s3FileStoreInfo.getNumPartitionedPrefix()));
                }
                if (s3FileStoreInfo.getPathStyleAccess() == 1) {
                    params.put(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS,
                            Boolean.toString(true));
                } else if (s3FileStoreInfo.getPathStyleAccess() == 2) {
                    params.put(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS,
                            Boolean.toString(false));
                }
                AwsCredentialInfo credentialInfo = s3FileStoreInfo.getCredential();
                if (credentialInfo.hasSimpleCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY,
                            credentialInfo.getSimpleCredential().getAccessKey());
                    params.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY,
                            credentialInfo.getSimpleCredential().getAccessKeySecret());
                } else if (credentialInfo.hasAssumeRoleCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "true");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN,
                            credentialInfo.getAssumeRoleCredential().getIamRoleArn());
                    params.put(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID,
                            credentialInfo.getAssumeRoleCredential().getExternalId());
                } else if (credentialInfo.hasProfileCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "true");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE, "false");
                } else if (credentialInfo.hasWebIdentityCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE, "true");
                    params.put(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN,
                            credentialInfo.getWebIdentityCredential().getIamRoleArn());
                    params.put(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID,
                            credentialInfo.getWebIdentityCredential().getExternalId());
                } else if (credentialInfo.hasDefaultCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
                }
                return params;
            case HDFS:
                HDFSFileStoreInfo hdfsFileStoreInfo = fsInfo.getHdfsFsInfo();
                params.putAll(hdfsFileStoreInfo.getConfigurationMap());
                String userName = hdfsFileStoreInfo.getUsername();
                if (!Strings.isNullOrEmpty(userName)) {
                    params.put(CloudConfigurationConstants.HDFS_USERNAME_DEPRECATED, userName);
                }
                return params;
            case AZBLOB: {
                AzBlobFileStoreInfo azBlobFileStoreInfo = fsInfo.getAzblobFsInfo();
                params.put(CloudConfigurationConstants.AZURE_BLOB_ENDPOINT, azBlobFileStoreInfo.getEndpoint());
                AzBlobCredentialInfo azBlobcredentialInfo = azBlobFileStoreInfo.getCredential();
                String sharedKey = azBlobcredentialInfo.getSharedKey();
                if (!Strings.isNullOrEmpty(sharedKey)) {
                    params.put(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, sharedKey);
                }
                String sasToken = azBlobcredentialInfo.getSasToken();
                if (!Strings.isNullOrEmpty(sasToken)) {
                    params.put(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, sasToken);
                }
                return params;
            }
            case ADLS2: {
                ADLS2FileStoreInfo adls2FileStoreInfo = fsInfo.getAdls2FsInfo();
                params.put(CloudConfigurationConstants.AZURE_ADLS2_ENDPOINT, adls2FileStoreInfo.getEndpoint());
                ADLS2CredentialInfo adls2credentialInfo = adls2FileStoreInfo.getCredential();
                String sharedKey = adls2credentialInfo.getSharedKey();
                if (!Strings.isNullOrEmpty(sharedKey)) {
                    params.put(CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY, sharedKey);
                }
                String sasToken = adls2credentialInfo.getSasToken();
                if (!Strings.isNullOrEmpty(sasToken)) {
                    params.put(CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN, sasToken);
                }
                String tenantId = adls2credentialInfo.getTenantId();
                if (!Strings.isNullOrEmpty(tenantId)) {
                    params.put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_TENANT_ID, tenantId);
                }
                String clientId = adls2credentialInfo.getClientId();
                if (!Strings.isNullOrEmpty(clientId)) {
                    params.put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID, clientId);
                }
                String clientSecret = adls2credentialInfo.getClientSecret();
                if (!Strings.isNullOrEmpty(clientSecret)) {
                    params.put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET, clientSecret);
                }
                // The file store has no field saying which OAuth2 flow was used, so it is inferred: a
                // tenant and a client with no secret is a managed identity, the same three with a
                // secret is a service principal. Inferring a managed identity from the tenant and the
                // client alone made a stored service principal come back as a managed identity, since
                // the credential builder prefers a managed identity over the client secret.
                if (!Strings.isNullOrEmpty(tenantId) && !Strings.isNullOrEmpty(clientId)
                        && Strings.isNullOrEmpty(clientSecret)) {
                    params.put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY, "true");
                }
                String clientEndpoint = adls2credentialInfo.getAuthorityHost();
                if (!Strings.isNullOrEmpty(clientEndpoint)) {
                    params.put(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT, clientEndpoint);
                }
                return params;
            }
            case GS: {
                GSFileStoreInfo gsFileStoreInfo = fsInfo.getGsFsInfo();
                params.put(CloudConfigurationConstants.GCP_GCS_ENDPOINT, gsFileStoreInfo.getEndpoint());
                if (gsFileStoreInfo.getUseComputeEngineServiceAccount()) {
                    params.put(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT, "true");
                } else {
                    params.put(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT, "false");
                    params.put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_EMAIL,
                            gsFileStoreInfo.getServiceAccountEmail());
                    params.put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID,
                            gsFileStoreInfo.getServiceAccountPrivateKeyId());
                    params.put(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY,
                            gsFileStoreInfo.getServiceAccountPrivateKey());
                }
                if (!Strings.isNullOrEmpty(gsFileStoreInfo.getImpersonation())) {
                    params.put(CloudConfigurationConstants.GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT,
                            gsFileStoreInfo.getImpersonation());
                }
                return params;
            }
            default:
                return params;
        }
    }

    private void preprocessAuthenticationIfNeeded(Map<String, String> params) {
        if (svt == StorageVolumeType.AZBLOB) {
            String container = locations.get(0).split("/")[0];
            params.put(CloudConfigurationConstants.AZURE_BLOB_CONTAINER, container);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // Same derivation as the constructors, for the same reason: params does not carry the AZBLOB
        // container, so rebuilding from the raw map would fall through to the HDFS provider and mark
        // a usable volume unusable on every image or edit-log reload - which the guards would then
        // act on after each FE restart.
        Map<String, String> configurationParams = new HashMap<>(params);
        preprocessAuthenticationIfNeeded(configurationParams);
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(configurationParams);
        credentialUsable = isValidCloudConfiguration(svt, cloudConfiguration);
    }
}
