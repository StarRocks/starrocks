// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudProperty;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;

import java.util.LinkedList;
import java.util.List;
public class AWSCloudConfiguration implements CloudConfiguration {

    private AWSCloudCredential awsCloudCredential;

    private boolean enablePathStyleAccess = false;

    private boolean enableSSL = true;

    public AWSCloudConfiguration(AWSCloudCredential awsCloudCredential) {
        this.awsCloudCredential = awsCloudCredential;
    }

    public void setEnablePathStyleAccess(boolean enablePathStyleAccess) {
        this.enablePathStyleAccess = enablePathStyleAccess;
    }

    public void setEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
    }

    public AWSCloudCredential getAWSCloudCredential() {
        return this.awsCloudCredential;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        configuration.set("fs.s3a.path.style.access", String.valueOf(enablePathStyleAccess));
        configuration.set("fs.s3a.connection.ssl.enabled", String.valueOf(enableSSL));
        awsCloudCredential.applyToConfiguration(configuration);
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        tCloudConfiguration.setCloud_type(TCloudType.AWS);

        List<TCloudProperty> properties = new LinkedList<>();
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS,
                String.valueOf(enablePathStyleAccess)));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_ENABLE_SSL, String.valueOf(enableSSL)));
        awsCloudCredential.toThrift(properties);
        tCloudConfiguration.setCloud_properties(properties);
    }

    @Override
    public String toString() {
        return "AWSCloudConfiguration{" +
                "awsCloudCredential=" + awsCloudCredential +
                ", enablePathStyleAccess=" + enablePathStyleAccess +
                ", enableSSL=" + enableSSL +
                '}';
    }
}
