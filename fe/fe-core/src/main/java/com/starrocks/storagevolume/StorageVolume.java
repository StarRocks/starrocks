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

<<<<<<< HEAD
=======
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AzBlobCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FileStoreInfo;
<<<<<<< HEAD
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.hdfs.HDFSCloudCredential;
=======
import com.staros.proto.HDFSFileStoreInfo;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
<<<<<<< HEAD
import org.apache.parquet.Strings;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
<<<<<<< HEAD
=======
import java.net.URI;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

<<<<<<< HEAD
import static com.starrocks.credential.CloudConfigurationConstants.AZURE_BLOB_CONTAINER;
import static com.starrocks.credential.CloudConfigurationConstants.HDFS_AUTHENTICATION;

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
public class StorageVolume implements Writable, GsonPostProcessable {
    public enum StorageVolumeType {
        UNKNOWN,
        S3,
        HDFS,
        AZBLOB
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

    @SerializedName("p")
    private Map<String, String> params;

    @SerializedName("c")
    private String comment;

    @SerializedName("e")
    private boolean enabled;

    public static String CREDENTIAL_MASK = "******";

    private String dumpMaskedParams(Map<String, String> params) {
        Gson gson = new Gson();
        Map<String, String> maskedParams = new HashMap<>(params);
        addMaskForCredential(maskedParams);
        return gson.toJson(maskedParams);
    }

    public StorageVolume(String id, String name, String svt, List<String> locations,
<<<<<<< HEAD
                         Map<String, String> params, boolean enabled, String comment) {
=======
                         Map<String, String> params, boolean enabled, String comment) throws DdlException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        this.id = id;
        this.name = name;
        this.svt = toStorageVolumeType(svt);
        this.locations = new ArrayList<>(locations);
        this.comment = comment;
        this.enabled = enabled;
        this.params = new HashMap<>(params);
        Map<String, String> configurationParams = new HashMap<>(params);
        preprocessAuthenticationIfNeeded(configurationParams);
<<<<<<< HEAD
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(configurationParams);
        if (!isValidCloudConfiguration()) {
            throw new SemanticException("Storage params is not valid " + dumpMaskedParams(params));
        }
    }

    public StorageVolume(StorageVolume sv) {
=======
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(configurationParams, true);
        if (!isValidCloudConfiguration()) {
            throw new SemanticException("Storage params is not valid " + dumpMaskedParams(params));
        }
        validateStorageVolumeConstraints();
    }

    public StorageVolume(StorageVolume sv) throws DdlException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        this.id = sv.id;
        this.name = sv.name;
        this.svt = sv.svt;
        this.locations = new ArrayList<>(sv.locations);
        this.comment = sv.comment;
        this.enabled = sv.enabled;
<<<<<<< HEAD
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(sv.params);
        this.params = new HashMap<>(sv.params);
=======
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(sv.params, true);
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public void setCloudConfiguration(Map<String, String> params) {
        Map<String, String> newParams = new HashMap<>(this.params);
        newParams.putAll(params);
<<<<<<< HEAD
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(newParams);
=======
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(newParams, true);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (!isValidCloudConfiguration()) {
            throw new SemanticException("Storage params is not valid " + dumpMaskedParams(newParams));
        }
        this.params = newParams;
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

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }
<<<<<<< HEAD
=======
    public List<String> getLocations() {
        return locations;
    }

    public String getType() {
        return svt.toString();
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    private StorageVolumeType toStorageVolumeType(String svt) {
        switch (svt.toLowerCase()) {
            case "s3":
                return StorageVolumeType.S3;
            case "hdfs":
                return StorageVolumeType.HDFS;
            case "azblob":
                return StorageVolumeType.AZBLOB;
            default:
                return StorageVolumeType.UNKNOWN;
        }
    }

    private boolean isValidCloudConfiguration() {
        switch (svt) {
            case S3:
                return cloudConfiguration.getCloudType() == CloudType.AWS;
            case HDFS:
                return cloudConfiguration.getCloudType() == CloudType.HDFS;
            case AZBLOB:
                return cloudConfiguration.getCloudType() == CloudType.AZURE;
            default:
                return false;
        }
    }

    public static void addMaskForCredential(Map<String, String> params) {
        params.computeIfPresent(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AWS_S3_SECRET_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, (key, value) -> CREDENTIAL_MASK);
    }

    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(name,
<<<<<<< HEAD
                String.valueOf(svt.name()),
                String.valueOf(GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                        .getDefaultStorageVolumeId().equals(id)),
                String.valueOf(Strings.join(locations, ", ")),
=======
                svt.name(),
                String.valueOf(GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                        .getDefaultStorageVolumeId().equals(id)),
                Joiner.on(", ").join(locations),
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                dumpMaskedParams(params),
                String.valueOf(enabled),
                String.valueOf(comment)));
    }

    public static FileStoreInfo createFileStoreInfo(String name, String svt,
                                                    List<String> locations, Map<String, String> params,
<<<<<<< HEAD
                                                    boolean enabled, String comment) {
=======
                                                    boolean enabled, String comment) throws DdlException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        StorageVolume sv = new StorageVolume("", name, svt, locations, params, enabled, comment);
        return sv.toFileStoreInfo();
    }

    public FileStoreInfo toFileStoreInfo() {
<<<<<<< HEAD
        FileStoreInfo fsInfo = cloudConfiguration.toFileStoreInfo();
        FileStoreInfo.Builder builder = fsInfo.toBuilder();
        builder.setFsKey(id).setFsName(this.name).setComment(this.comment).setEnabled(this.enabled)
                .addAllLocations(locations).build();
        return builder.build();
    }

    public static StorageVolume fromFileStoreInfo(FileStoreInfo fsInfo) {
=======
        FileStoreInfo.Builder builder = cloudConfiguration.toFileStoreInfo().toBuilder();
        builder.setFsKey(id)
                .setFsName(this.name)
                .setComment(this.comment)
                .setEnabled(this.enabled)
                .addAllLocations(locations);
        return builder.build();
    }

    public static StorageVolume fromFileStoreInfo(FileStoreInfo fsInfo) throws DdlException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        String svt = fsInfo.getFsType().toString();
        Map<String, String> params = getParamsFromFileStoreInfo(fsInfo);
        return new StorageVolume(fsInfo.getFsKey(), fsInfo.getFsName(), svt,
                fsInfo.getLocationsList(), params, fsInfo.getEnabled(), fsInfo.getComment());
    }

    public static Map<String, String> getParamsFromFileStoreInfo(FileStoreInfo fsInfo) {
        Map<String, String> params = new HashMap<>();
        switch (fsInfo.getFsType()) {
            case S3:
                S3FileStoreInfo s3FileStoreInfo = fsInfo.getS3FsInfo();
                params.put(CloudConfigurationConstants.AWS_S3_REGION, s3FileStoreInfo.getRegion());
                params.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, s3FileStoreInfo.getEndpoint());
<<<<<<< HEAD
=======
                if (s3FileStoreInfo.getPartitionedPrefixEnabled()) {
                    // Don't show the parameters if not enabled.
                    params.put(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX,
                            Boolean.toString(true));
                    params.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX,
                            Integer.toString(s3FileStoreInfo.getNumPartitionedPrefix()));
                }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                AwsCredentialInfo credentialInfo = s3FileStoreInfo.getCredential();
                if (credentialInfo.hasSimpleCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY,
                            credentialInfo.getSimpleCredential().getAccessKey());
                    params.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY,
                            credentialInfo.getSimpleCredential().getAccessKeySecret());
                } else if (credentialInfo.hasAssumeRoleCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "true");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN,
                            credentialInfo.getAssumeRoleCredential().getIamRoleArn());
                    params.put(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID,
                            credentialInfo.getAssumeRoleCredential().getExternalId());
                } else if (credentialInfo.hasProfileCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "true");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                } else if (credentialInfo.hasDefaultCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
                }
                return params;
            case HDFS:
<<<<<<< HEAD
                // TODO
=======
                HDFSFileStoreInfo hdfsFileStoreInfo = fsInfo.getHdfsFsInfo();
                params.putAll(hdfsFileStoreInfo.getConfigurationMap());
                String userName = hdfsFileStoreInfo.getUsername();
                if (!Strings.isNullOrEmpty(userName)) {
                    params.put(CloudConfigurationConstants.HDFS_USERNAME_DEPRECATED, userName);
                }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                return params;
            case AZBLOB:
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
            default:
                return params;
        }
    }

    private void preprocessAuthenticationIfNeeded(Map<String, String> params) {
<<<<<<< HEAD
        if (svt == StorageVolumeType.HDFS) {
            params.computeIfAbsent(HDFS_AUTHENTICATION, key -> HDFSCloudCredential.EMPTY);
        } else if (svt == StorageVolumeType.AZBLOB) {
            String container = locations.get(0).split("/")[0];
            params.put(AZURE_BLOB_CONTAINER, container);
=======
        if (svt == StorageVolumeType.AZBLOB) {
            String container = locations.get(0).split("/")[0];
            params.put(CloudConfigurationConstants.AZURE_BLOB_CONTAINER, container);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static StorageVolume read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, StorageVolume.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(params);
    }
}
