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

package com.starrocks.load.loadv2.dpp;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ExpressionEncoderHelper implements Serializable {

    private ExpressionEncoder encoder;
    private transient Object instance = null;
    private transient Method toRowMethod = null;

    public ExpressionEncoderHelper(ExpressionEncoder encoder) {
        this.encoder = encoder;
    }

    /**
     * See SPARK-31450
     * Spark 3.0.0 remove toRow method.
     */
    private void initMethodIfNeeded() {
        if (toRowMethod != null) {
            return;
        }
        Class<? extends ExpressionEncoder> encoderClass = encoder.getClass();
        try {
            toRowMethod = encoderClass.getMethod("toRow", Object.class);
            instance = encoder;
        } catch (NoSuchMethodException e) {
            try {
                Method serializerMethod = encoderClass.getMethod("createSerializer");
                Object serializer = serializerMethod.invoke(encoder);
                toRowMethod = serializer.getClass().getMethod("apply", Object.class);
                instance = serializer;
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public InternalRow toRow(Object obj) {
        initMethodIfNeeded();
        try {
            return (InternalRow) toRowMethod.invoke(instance, obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
