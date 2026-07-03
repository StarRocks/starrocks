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

package com.starrocks.planner.lance;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.LanceTable;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LanceConfigTest {
    @Test
    public void testBuildS3StorageOptions() {
        Map<String, String> options = LanceConfig.buildStorageOptions(ImmutableMap.of(
                CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "ak",
                CloudConfigurationConstants.AWS_S3_SECRET_KEY, "sk",
                CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, "token",
                CloudConfigurationConstants.AWS_S3_ENDPOINT, "https://s3.test",
                CloudConfigurationConstants.AWS_S3_REGION, "us-east-1",
                CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS, "true",
                LanceConfig.PROP_RAW_OPTION_PREFIX + "aws_allow_http", "true"));

        Assertions.assertEquals("ak", options.get("aws_access_key_id"));
        Assertions.assertEquals("sk", options.get("aws_secret_access_key"));
        Assertions.assertEquals("token", options.get("aws_session_token"));
        Assertions.assertEquals("https://s3.test", options.get("aws_endpoint"));
        Assertions.assertEquals("us-east-1", options.get("aws_region"));
        Assertions.assertEquals("false", options.get("aws_virtual_hosted_style_request"));
        Assertions.assertEquals("true", options.get("aws_allow_http"));
    }

    @Test
    public void testBuildOssStorageOptions() {
        Map<String, String> options = LanceConfig.buildStorageOptions(ImmutableMap.of(
                CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY, "oss-ak",
                CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY, "oss-sk",
                CloudConfigurationConstants.ALIYUN_OSS_STS_TOKEN, "oss-token",
                CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT, "oss-cn-beijing.aliyuncs.com",
                CloudConfigurationConstants.ALIYUN_OSS_REGION, "cn-beijing"));

        Assertions.assertEquals("oss-ak", options.get("aws_access_key_id"));
        Assertions.assertEquals("oss-sk", options.get("aws_secret_access_key"));
        Assertions.assertEquals("oss-token", options.get("aws_session_token"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", options.get("aws_endpoint"));
        Assertions.assertEquals("cn-beijing", options.get("aws_region"));
    }

    @Test
    public void testLanceScanNodeUsesConnectorScheduler() {
        LanceTable table = new LanceTable("lance_catalog", "default", "tbl", List.of(),
                ImmutableMap.of(LanceTable.DATASET_URI, "file:///tmp/tbl.lance"));
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(0));
        tupleDescriptor.setTable(table);

        LanceScanNode scanNode = new LanceScanNode(new PlanNodeId(0), tupleDescriptor, "LanceScanNode");

        Assertions.assertTrue(scanNode.isConnectorScanNode());
    }

    @Test
    public void testLanceReaderSessionSwitch() {
        SessionVariable sessionVariable = new SessionVariable();

        Assertions.assertTrue(LanceScanNode.useNativeReader(sessionVariable));

        sessionVariable.setLanceForceJNIReader(true);
        Assertions.assertFalse(LanceScanNode.useNativeReader(sessionVariable));

        sessionVariable.setLanceForceNativeReader(true);
        Assertions.assertFalse(LanceScanNode.useNativeReader(sessionVariable));
    }
}
