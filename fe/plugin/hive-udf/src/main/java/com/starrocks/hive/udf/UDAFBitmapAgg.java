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
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

// This function similar to the aggregate function(bitmap_agg) of StarRocks
public class UDAFBitmapAgg extends AbstractGenericUDAFResolver {
    @Override
    public BitmapAggEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            throw new UDFArgumentException("Argument number of bitmap_agg should be 1.");
        }
        return new BitmapAggEvaluator();
    }

    public static class BitmapAggEvaluator extends GenericUDAFEvaluator {
        private transient PrimitiveObjectInspector inputInspector;
        private transient BinaryObjectInspector mergeInspector;

        @AggregationType(estimable = true)
        static class BitmapAggBuffer extends AbstractAggregationBuffer {
            BitmapValue bitmap;

            BitmapAggBuffer() {
                bitmap = new BitmapValue();
            }
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentException("Argument number of bitmap_agg should be 1.");
            }
            super.init(m, parameters);
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                this.inputInspector = (PrimitiveObjectInspector) parameters[0];
            } else {
                this.mergeInspector = (BinaryObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() {
            return new BitmapAggBuffer();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) {
            ((BitmapAggBuffer) aggregationBuffer).bitmap = new BitmapValue();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            BitmapAggBuffer buf = (BitmapAggBuffer) aggregationBuffer;
            try {
                for (Object obj : objects) {
                    if (obj != null) {
                        long row = PrimitiveObjectInspectorUtils.getLong(obj, inputInspector);
                        buf.bitmap.add(row);
                    }
                }
            } catch (NumberFormatException e) {
                throw new HiveException(e);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            BitmapAggBuffer buf = (BitmapAggBuffer) aggregationBuffer;
            try {
                return BitmapValue.bitmapToBytes(buf.bitmap);
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            BitmapAggBuffer buf = (BitmapAggBuffer) aggregationBuffer;
            byte[] tmpBuf = this.mergeInspector.getPrimitiveJavaObject(o);
            try {
                buf.bitmap.or(BitmapValue.bitmapFromBytes(tmpBuf));
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return terminate(aggregationBuffer);
        }
    }
}
