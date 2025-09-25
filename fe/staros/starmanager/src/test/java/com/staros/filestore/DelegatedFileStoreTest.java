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

package com.staros.filestore;

import com.staros.credential.AwsDefaultCredential;
import com.staros.util.Constant;
import org.junit.Assert;
import org.junit.Test;

public class DelegatedFileStoreTest {

    @Test
    public void testDelegateFileStoreInterface() {
        FileStore s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region", "test-endpoint", new AwsDefaultCredential(), "");

        DelegatedFileStore dFileStore = new DelegatedFileStore(s3fs);

        Assert.assertEquals(s3fs.getComment(), dFileStore.getComment());
        Assert.assertEquals(s3fs.getEnabled(), dFileStore.getEnabled());
        Assert.assertEquals(s3fs.isValid(), dFileStore.isValid());
        Assert.assertEquals(s3fs.key(), dFileStore.key());
        Assert.assertEquals(s3fs.name(), dFileStore.name());
        Assert.assertEquals(s3fs.getVersion(), dFileStore.getVersion());
        Assert.assertEquals(s3fs.isBuiltin(), dFileStore.isBuiltin());
        Assert.assertEquals(s3fs.rootPath(), dFileStore.rootPath());
        Assert.assertEquals(s3fs.type(), dFileStore.type());
        Assert.assertEquals(s3fs.toProtobuf(), dFileStore.toProtobuf());
        Assert.assertEquals(s3fs.toDebugProtobuf(), dFileStore.toDebugProtobuf());

        dFileStore.increaseVersion();
        Assert.assertEquals(s3fs.getVersion(), dFileStore.getVersion());

        dFileStore.setEnabled(false);
        Assert.assertFalse(s3fs.getEnabled());
        Assert.assertFalse(s3fs.isBuiltin());
        dFileStore.setBuiltin(!dFileStore.isBuiltin());
        Assert.assertTrue(s3fs.isBuiltin());

        FileStore other = FileStore.fromProtobuf(s3fs.toProtobuf());
        other.setEnabled(!other.getEnabled());
        Assert.assertNotEquals(other.getEnabled(), s3fs.getEnabled());

        dFileStore.mergeFrom(other);
        Assert.assertEquals(s3fs.getEnabled(), other.getEnabled());
        Assert.assertEquals(s3fs.getVersion(), other.getVersion());
        Assert.assertEquals(dFileStore.getVersion(), s3fs.getVersion());

        // switch to a different delegation, breaks the link between s3fs <-> dFileStore
        dFileStore.swapDelegation(s3fs.toProtobuf());
        dFileStore.increaseVersion();
        Assert.assertNotEquals(s3fs.getVersion(), dFileStore.getVersion());
    }
}
