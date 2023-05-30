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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.server.GlobalStateMgr;
import org.apache.parquet.Strings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageVolume {
    public static final String S3_PREFIX = "s3://";

    public enum StorageVolumeType {
        UNKNOWN,
        S3,
        HDFS
    }

    // Without id, the scenario like "create storage volume 'a', drop storage volume 'a', create storage volume 'a'"
    // can not be handled. They will be treated as the same storage volume.
    private String id;

    private String name;

    private StorageVolumeType svt;

    private List<String> locations;

    private CloudConfiguration cloudConfiguration;

    private Map<String, String> params;

    private String comment;

    private boolean enabled;

    public StorageVolume(String id, String name, String svt, List<String> locations,
                         Map<String, String> params, boolean enabled, String comment) throws AnalysisException {
        this.id = id;
        this.name = name;
        this.svt = toStorageVolumeType(svt);
        this.locations = new ArrayList<>(locations);
        this.comment = comment;
        this.enabled = enabled;
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(params);
        if (!isValidCloudConfiguration()) {
            throw new AnalysisException("Storage params is not valid");
        }
        this.params = new HashMap<>(params);
    }

    public StorageVolume(StorageVolume sv) {
        this.id = sv.id;
        this.name = sv.name;
        this.svt = sv.svt;
        this.locations = new ArrayList<>(sv.locations);
        this.comment = sv.comment;
        this.enabled = sv.enabled;
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(sv.params);
        this.params = new HashMap<>(sv.params);
    }

    public void setCloudConfiguration(Map<String, String> params) throws AnalysisException {
        Map<String, String> newParams = new HashMap<>(this.params);
        newParams.putAll(params);
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(newParams);
        if (!isValidCloudConfiguration()) {
            throw new AnalysisException("Storage params is not valid");
        }
        this.params = newParams;
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }

    public String getId() {
        return id;
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

    private StorageVolumeType toStorageVolumeType(String svt) {
        switch (svt.toLowerCase()) {
            case "s3":
                return StorageVolumeType.S3;
            case "hdfs":
                return StorageVolumeType.HDFS;
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
            default:
                return false;
        }
    }

    public void getProcNodeData(BaseProcResult result) {
        Gson gson = new Gson();
        result.addRow(Lists.newArrayList(name,
                String.valueOf(svt.name()),
                String.valueOf(GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                        .getDefaultStorageVolumeId().equals(id)),
                String.valueOf(Strings.join(locations, ", ")),
                String.valueOf(gson.toJson(params)),
                String.valueOf(enabled),
                String.valueOf(comment)));
    }

    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo fsInfo = cloudConfiguration.toFileStoreInfo();
        FileStoreInfo.Builder builder = fsInfo.toBuilder();
        builder.setFsKey(id).setFsName(this.name).setComment(this.comment).setEnabled(this.enabled);
        switch (svt) {
            case S3:
                S3FileStoreInfo s3FileStoreInfo = fsInfo.getS3FsInfo();
                String[] bucketAndPrefix = getBucketAndPrefix();
                s3FileStoreInfo = s3FileStoreInfo.toBuilder()
                        .setBucket(bucketAndPrefix[0]).setPathPrefix(bucketAndPrefix[1]).build();
                builder.setS3FsInfo(s3FileStoreInfo);
                break;
            case HDFS:
                // TODO
                break;
            case UNKNOWN:
                break;
        }
        return builder.build();
    }

    public static StorageVolume fromFileStoreInfo(FileStoreInfo fsInfo) throws AnalysisException {
        String svt = fsInfo.getFsType().toString();
        Map<String, String> params = getParamsFromFileStoreInfo(fsInfo);
        List<String> locations = getLocationsFromFileStoreInfo(fsInfo);
        return new StorageVolume(fsInfo.getFsKey(), fsInfo.getFsName(), svt,
                locations, params, fsInfo.getEnabled(), fsInfo.getComment());
    }

    public static List<String> getLocationsFromFileStoreInfo(FileStoreInfo fsInfo) {
        switch (fsInfo.getFsType()) {
            case S3:
                return new ArrayList<>(Arrays.asList(
                        "S3://" + fsInfo.getS3FsInfo().getBucket() + "/" + fsInfo.getS3FsInfo().getPathPrefix()));
            case HDFS:
                // TODO
            case AZBLOB:
                // TODO
            default:
                return new ArrayList<>();
        }
    }

    public static Map<String, String> getParamsFromFileStoreInfo(FileStoreInfo fsInfo) {
        Map<String, String> params = new HashMap<>();
        switch (fsInfo.getFsType()) {
            case S3:
                S3FileStoreInfo s3FileStoreInfo = fsInfo.getS3FsInfo();
                params.put(CloudConfigurationConstants.AWS_S3_REGION, s3FileStoreInfo.getRegion());
                params.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, s3FileStoreInfo.getEndpoint());
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
                // TODO
            case AZBLOB:
                // TODO
            default:
                return params;
        }
    }

    private String[] getBucketAndPrefix() {
        // path pattern: s3://default-bucket/1/12003/
        String path = locations.get(0).substring(S3_PREFIX.length());
        int index = path.indexOf('/');
        if (index < 0) {
            return new String[] {path, ""};
        }

        return new String[] {path.substring(0, index), path.substring(index + 1)};
    }
}
