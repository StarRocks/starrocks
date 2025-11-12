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

package com.starrocks.hive.udf;

import com.starrocks.types.BitmapValue;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.io.IOException;

public class UDFBitmapAnd extends GenericUDF {
    private transient BinaryObjectInspector inspector0;
    private transient BinaryObjectInspector inspector1;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException("Argument number of bitmap_and should be 2");
        }

        ObjectInspector arg0 = args[0];
        ObjectInspector arg1 = args[1];
        if (!(arg0 instanceof BinaryObjectInspector) || !(arg1 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("Argument of bitmap_and should be binary type");
        }
        this.inspector0 = (BinaryObjectInspector) arg0;
        this.inspector1 = (BinaryObjectInspector) arg1;

        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args[0].get() == null || args[1].get() == null) {
            return null;
        }

        byte[] bytes0 = PrimitiveObjectInspectorUtils.getBinary(args[0].get(), this.inspector0).getBytes();
        byte[] bytes1 = PrimitiveObjectInspectorUtils.getBinary(args[1].get(), this.inspector1).getBytes();

        try {
            BitmapValue bitmap0 = BitmapValue.bitmapFromBytes(bytes0);
            BitmapValue bitmap1 = BitmapValue.bitmapFromBytes(bytes1);
            bitmap0.and(bitmap1);
            return BitmapValue.bitmapToBytes(bitmap0);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "USAGE: bitmap_and(bitmap, bitmap)";
    }
}
