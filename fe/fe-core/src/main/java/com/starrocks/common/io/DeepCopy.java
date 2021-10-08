// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/io/DeepCopy.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.io;

import com.starrocks.common.FeConstants;
import com.starrocks.meta.MetaContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;

/*
 * This class is for deep copying a writable instance.
 */
public class DeepCopy {
    private static final Logger LOG = LogManager.getLogger(DeepCopy.class);

    public static final String READ_METHOD_NAME = "readFields";

    // deep copy orig to dest.
    // the param "c" is the implementation class of "dest".
    // And the "dest" class must has method "readFields(DataInput)"
    public static boolean copy(Writable orig, Writable dest, Class c) {
        // Backup the current MetaContext before assigning a new one.
        MetaContext oldContext = MetaContext.get();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeConstants.meta_version);
        metaContext.setStarRocksMetaVersion(FeConstants.starrocks_meta_version);
        metaContext.setThreadLocalInfo();

        FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
        try {
            orig.write(out);
            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());

            Method readMethod = c.getDeclaredMethod(READ_METHOD_NAME, DataInput.class);
            readMethod.invoke(dest, in);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("failed to copy object.", e);
            return false;
        } finally {
            // Restore the old MetaContext.
            if (oldContext != null) {
                oldContext.setThreadLocalInfo();
            } else {
                MetaContext.remove();
            }
        }
        return true;
    }
}
