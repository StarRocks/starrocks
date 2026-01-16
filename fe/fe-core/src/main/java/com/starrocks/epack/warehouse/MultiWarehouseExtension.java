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

package com.starrocks.epack.warehouse;

import com.starrocks.epack.warehouse.cngroup.CNGroupResource;
import com.starrocks.extension.ExtensionContext;
import com.starrocks.extension.SRModule;
import com.starrocks.extension.StarRocksExtension;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;
import com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.Map;

@SRModule(name = "multi_warehouse_extension")
public class MultiWarehouseExtension implements StarRocksExtension {
    @Override
    public void onLoad(ExtensionContext ctx) {
        registerPersist();

        ctx.registerConstructor(WarehouseManager.class, WarehouseManagerEPack.class);
        ctx.registerConstructor(BaseSlotManager.class, WarehouseSlotManager.class);
    }

    void registerPersist() {
        // MultiWarehouse persist
        final Map<Class<?>, RuntimeTypeAdapterFactory<?>> clazz2Factories =
                RuntimeTypeAdapterTypes.CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES;
        final RuntimeTypeAdapterFactory<ComputeResource> compute_resource_runtime_type_adapter_factory =
                (RuntimeTypeAdapterFactory<ComputeResource>) clazz2Factories.get(ComputeResource.class);
        compute_resource_runtime_type_adapter_factory.registerSubtype(CNGroupResource.class, "CNGroupResource", true);

        final RuntimeTypeAdapterFactory<Warehouse> warehouse_type_adapter_factory =
                (RuntimeTypeAdapterFactory<Warehouse>) clazz2Factories.get(Warehouse.class);
        warehouse_type_adapter_factory.registerSubtype(LocalWarehouse.class, "LocalWarehouse");
    }
}