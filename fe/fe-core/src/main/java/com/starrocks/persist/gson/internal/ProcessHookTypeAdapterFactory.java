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

package com.starrocks.persist.gson.internal;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;

import java.io.IOException;

public class ProcessHookTypeAdapterFactory implements TypeAdapterFactory {

    public ProcessHookTypeAdapterFactory() {
    }

    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);

        return new TypeAdapter<T>() {
            public void write(JsonWriter out, T obj) throws IOException {
                // Use Object lock to protect its properties from changed by other serialize thread.
                // But this will only take effect when all threads uses GSONUtils to serialize object,
                // because other methods of changing properties do not necessarily require the acquisition of the object lock
                if (obj instanceof GsonPreProcessable) {
                    synchronized (obj) {
                        ((GsonPreProcessable) obj).gsonPreProcess();
                        delegate.write(out, obj);
                    }
                } else {
                    delegate.write(out, obj);
                }
            }

            public T read(JsonReader reader) throws IOException {
                T obj = delegate.read(reader);
                if (obj instanceof GsonPostProcessable) {
                    ((GsonPostProcessable) obj).gsonPostProcess();
                }
                return obj;
            }
        };
    }
}

