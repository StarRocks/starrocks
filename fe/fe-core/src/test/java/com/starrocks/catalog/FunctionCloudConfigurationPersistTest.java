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

package com.starrocks.catalog;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class FunctionCloudConfigurationPersistTest {
    private ScalarFunction newUdf(Map<String, String> properties) {
        CloudConfiguration configuration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Type varchar = TypeFactory.createVarcharType(65533);
        return ScalarFunction.createUdf(new FunctionName("", "cloudcfg_repro"), new Type[] {varchar}, varchar,
                false, TFunctionBinaryType.SRJAR, "s3://bucket/reverse.jar", "test.ReverseString", "", "",
                configuration);
    }

    @Test
    public void testAwsConfigurationSurvivesFunctionJsonRoundTrip() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(AWS_S3_ACCESS_KEY, "test-access-key");
        properties.put(AWS_S3_SECRET_KEY, "test-secret-key");
        properties.put(AWS_S3_REGION, "us-west-2");
        properties.put(AWS_S3_ENDPOINT, "http://localhost:19080");
        ScalarFunction original = newUdf(properties);

        String json = GsonUtils.GSON.toJson(original);
        Assertions.assertTrue(json.contains("cloud_configuration_properties"));
        Assertions.assertFalse(json.contains("hadoop.cloud.configuration.string"));
        Function restored = GsonUtils.GSON.fromJson(json, Function.class);
        Assertions.assertEquals(CloudType.AWS, restored.getCloudConfiguration().getCloudType());

        TFunction originalThrift = original.toThrift();
        TFunction restoredThrift = restored.toThrift();
        Assertions.assertEquals(originalThrift.getCloud_configuration(), restoredThrift.getCloud_configuration());
        new TSerializer(new TBinaryProtocol.Factory()).serialize(restoredThrift);
        Assertions.assertEquals(restoredThrift.getCloud_configuration(),
                restored.copy().toThrift().getCloud_configuration());
    }

    @Test
    public void testDefaultCredentialModeSurvivesFunctionJsonRoundTrip() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        properties.put(AWS_S3_REGION, "us-west-2");
        properties.put(AWS_S3_ENDPOINT, "s3.us-west-2.amazonaws.com");
        ScalarFunction original = newUdf(properties);

        Function restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), Function.class);
        Assertions.assertEquals(original.toThrift().getCloud_configuration(),
                restored.toThrift().getCloud_configuration());
        new TSerializer(new TBinaryProtocol.Factory()).serialize(restored.toThrift());
    }

    @Test
    public void testOldFunctionMetadataRequiresRecreation() {
        Map<String, String> properties = Map.of(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        JsonObject oldMetadata = JsonParser.parseString(GsonUtils.GSON.toJson(newUdf(properties))).getAsJsonObject();
        oldMetadata.remove("cloud_configuration_properties");
        oldMetadata.add("cloud_configuration", new JsonObject());

        Function restored = GsonUtils.GSON.fromJson(oldMetadata, Function.class);
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, restored::toThrift);
        Assertions.assertTrue(exception.getMessage().contains("recreate the function"));
    }
}
