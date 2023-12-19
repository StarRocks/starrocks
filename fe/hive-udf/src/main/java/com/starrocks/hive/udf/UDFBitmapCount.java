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

// This function similar to the function(bitmap_count) of StarRocks
public class UDFBitmapCount extends GenericUDF {
    private transient BinaryObjectInspector inspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("Argument number of bitmap_count should be 1");
        }

        ObjectInspector arg0 = args[0];
        if (!(arg0 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("First argument should be binary type");
        }
        this.inspector = (BinaryObjectInspector) arg0;

        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args[0] == null) {
            return null;
        }

        byte[] bytes = PrimitiveObjectInspectorUtils.getBinary(args[0].get(), this.inspector).getBytes();
        try {
            BitmapValue bitmap = BitmapValue.bitmapFromBytes(bytes);
            return bitmap.cardinality();
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "USAGE: bitmap_count(bitmap)";
    }
}
