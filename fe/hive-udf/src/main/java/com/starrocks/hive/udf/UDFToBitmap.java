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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.IOException;

// This function similar to the function(to_bitmap) of StarRocks
public class UDFToBitmap extends GenericUDF {

    private transient LongObjectInspector inspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1)
            throw new UDFArgumentException("Argument number of to_bitmap should be 1");
        ObjectInspector arg0 = args[0];
        if (!(arg0 instanceof LongObjectInspector))
            throw new UDFArgumentException("First argument should be bigint type");
        this.inspector = (LongObjectInspector) args[0];
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args[0].get() == null)
            return null;
        try {
            long val = this.inspector.get(args[0].get());
            if (val < 0) {
                throw new IllegalArgumentException("to_bitmap only support bigint value from 0 to 18446744073709551615, but got negative value: " + val);
            }
            return BitmapValue.bitmapToBytes(new BitmapValue(val));
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] strings) {
        return "USAGE: to_bitmap(bigint)";
    }

}
