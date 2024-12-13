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

package com.starrocks.lake.compaction;

<<<<<<< HEAD
=======
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
<<<<<<< HEAD
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
=======
import java.io.DataInputStream;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.io.DataOutputStream;
import java.io.IOException;

public class CompactionTxnCommitAttachmentTest {
    @Test
    public void testBasic() throws IOException {
        CompactionTxnCommitAttachment attachment = new CompactionTxnCommitAttachment();
        Assert.assertFalse(attachment.getForceCommit());

        CompactionTxnCommitAttachment attachment2 = new CompactionTxnCommitAttachment(true /* forceCommit */);
        Assert.assertTrue(attachment2.getForceCommit());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);
<<<<<<< HEAD
        attachment2.write((DataOutput) out);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bout.toByteArray()));
        attachment.readFields((DataInput) in);
=======
        Text.writeString(out, GsonUtils.GSON.toJson(attachment2));
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bout.toByteArray()));
        attachment = GsonUtils.GSON.fromJson(Text.readString(in), CompactionTxnCommitAttachment.class);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Assert.assertTrue(attachment.getForceCommit());
    }
}
