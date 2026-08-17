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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.scheduler.TaskRun;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class MVRefreshParamsTest {

    @Test
    public void testIsForceWithConfigNonPartitioned() {
        // Save original config value
        int originalValue = Config.mv_refresh_force_partition_type;
        
        try {
            // Test non-partitioned MV with config = 1
            Config.mv_refresh_force_partition_type = 1;
            
            MaterializedView mv = Mockito.mock(MaterializedView.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(partitionInfo.isUnPartitioned()).thenReturn(true);
            Mockito.when(partitionInfo.isRangePartition()).thenReturn(false);
            Mockito.when(partitionInfo.isListPartition()).thenReturn(false);
            Mockito.when(mv.getPartitionRefreshStrategy()).thenReturn(MaterializedView.PartitionRefreshStrategy.STRICT);
            
            Map<String, String> properties = new HashMap<>();
            MVRefreshParams params = new MVRefreshParams(mv, properties);
            
            Assertions.assertTrue(params.isForce(), "Non-partitioned MV should be forced when config=1");
            
            // Test with config = 0 (disabled)
            Config.mv_refresh_force_partition_type = 0;
            Assertions.assertFalse(params.isForce(), "Non-partitioned MV should not be forced when config=0");
            
        } finally {
            // Restore original config value
            Config.mv_refresh_force_partition_type = originalValue;
        }
    }

    @Test
    public void testIsForceWithConfigRangePartitioned() {
        // Save original config value
        int originalValue = Config.mv_refresh_force_partition_type;
        
        try {
            // Test range partitioned MV with config = 2
            Config.mv_refresh_force_partition_type = 2;
            
            MaterializedView mv = Mockito.mock(MaterializedView.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(partitionInfo.isUnPartitioned()).thenReturn(false);
            Mockito.when(partitionInfo.isRangePartition()).thenReturn(true);
            Mockito.when(partitionInfo.isListPartition()).thenReturn(false);
            Mockito.when(mv.getPartitionRefreshStrategy()).thenReturn(MaterializedView.PartitionRefreshStrategy.STRICT);
            
            Map<String, String> properties = new HashMap<>();
            MVRefreshParams params = new MVRefreshParams(mv, properties);
            
            Assertions.assertTrue(params.isForce(), "Range partitioned MV should be forced when config=2");
            
            // Test with config = 1 (only non-partitioned)
            Config.mv_refresh_force_partition_type = 1;
            Assertions.assertFalse(params.isForce(), "Range partitioned MV should not be forced when config=1");
            
        } finally {
            // Restore original config value
            Config.mv_refresh_force_partition_type = originalValue;
        }
    }

    @Test
    public void testIsForceWithConfigListPartitioned() {
        // Save original config value
        int originalValue = Config.mv_refresh_force_partition_type;
        
        try {
            // Test list partitioned MV with config = 4
            Config.mv_refresh_force_partition_type = 4;
            
            MaterializedView mv = Mockito.mock(MaterializedView.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(partitionInfo.isUnPartitioned()).thenReturn(false);
            Mockito.when(partitionInfo.isRangePartition()).thenReturn(false);
            Mockito.when(partitionInfo.isListPartition()).thenReturn(true);
            Mockito.when(mv.getPartitionRefreshStrategy()).thenReturn(MaterializedView.PartitionRefreshStrategy.STRICT);
            
            Map<String, String> properties = new HashMap<>();
            MVRefreshParams params = new MVRefreshParams(mv, properties);
            
            Assertions.assertTrue(params.isForce(), "List partitioned MV should be forced when config=4");
            
            // Test with config = 3 (non-partitioned + range)
            Config.mv_refresh_force_partition_type = 3;
            Assertions.assertFalse(params.isForce(), "List partitioned MV should not be forced when config=3");
            
        } finally {
            // Restore original config value
            Config.mv_refresh_force_partition_type = originalValue;
        }
    }

    @Test
    public void testIsForceWithConfigCombinedValue() {
        // Save original config value
        int originalValue = Config.mv_refresh_force_partition_type;
        
        try {
            // Test with config = 7 (all types: 1+2+4)
            Config.mv_refresh_force_partition_type = 7;
            
            MaterializedView mv = Mockito.mock(MaterializedView.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(mv.getPartitionRefreshStrategy()).thenReturn(MaterializedView.PartitionRefreshStrategy.STRICT);
            
            Map<String, String> properties = new HashMap<>();
            
            // Test non-partitioned
            Mockito.when(partitionInfo.isUnPartitioned()).thenReturn(true);
            Mockito.when(partitionInfo.isRangePartition()).thenReturn(false);
            Mockito.when(partitionInfo.isListPartition()).thenReturn(false);
            MVRefreshParams params = new MVRefreshParams(mv, properties);
            Assertions.assertTrue(params.isForce(), "Non-partitioned MV should be forced when config=7");
            
            // Test range partitioned
            Mockito.when(partitionInfo.isUnPartitioned()).thenReturn(false);
            Mockito.when(partitionInfo.isRangePartition()).thenReturn(true);
            Mockito.when(partitionInfo.isListPartition()).thenReturn(false);
            params = new MVRefreshParams(mv, properties);
            Assertions.assertTrue(params.isForce(), "Range partitioned MV should be forced when config=7");
            
            // Test list partitioned
            Mockito.when(partitionInfo.isUnPartitioned()).thenReturn(false);
            Mockito.when(partitionInfo.isRangePartition()).thenReturn(false);
            Mockito.when(partitionInfo.isListPartition()).thenReturn(true);
            params = new MVRefreshParams(mv, properties);
            Assertions.assertTrue(params.isForce(), "List partitioned MV should be forced when config=7");
            
        } finally {
            // Restore original config value
            Config.mv_refresh_force_partition_type = originalValue;
        }
    }

    @Test
    public void testIsForceWithExplicitForceProperty() {
        // Save original config value
        int originalValue = Config.mv_refresh_force_partition_type;
        
        try {
            // Even with config = 0, explicit FORCE property should work
            Config.mv_refresh_force_partition_type = 0;
            
            MaterializedView mv = Mockito.mock(MaterializedView.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(mv.getPartitionRefreshStrategy()).thenReturn(MaterializedView.PartitionRefreshStrategy.STRICT);
            
            Map<String, String> properties = new HashMap<>();
            properties.put(TaskRun.FORCE, "true");
            
            MVRefreshParams params = new MVRefreshParams(mv, properties);
            Assertions.assertTrue(params.isForce(), "MV should be forced when FORCE property is true");
            
        } finally {
            // Restore original config value
            Config.mv_refresh_force_partition_type = originalValue;
        }
    }

    @Test
    public void testIsForceWithPartitionRefreshStrategy() {
        // Save original config value
        int originalValue = Config.mv_refresh_force_partition_type;
        
        try {
            // Even with config = 0, FORCE strategy should work
            Config.mv_refresh_force_partition_type = 0;
            
            MaterializedView mv = Mockito.mock(MaterializedView.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(mv.getPartitionRefreshStrategy()).thenReturn(MaterializedView.PartitionRefreshStrategy.FORCE);
            
            Map<String, String> properties = new HashMap<>();
            MVRefreshParams params = new MVRefreshParams(mv, properties);
            
            Assertions.assertTrue(params.isForce(), "MV should be forced when PartitionRefreshStrategy is FORCE");
            
        } finally {
            // Restore original config value
            Config.mv_refresh_force_partition_type = originalValue;
        }
    }

    @Test
    public void testIsNonTentativeForce() {
        // Save original config value
        int originalValue = Config.mv_refresh_force_partition_type;
        
        try {
            Config.mv_refresh_force_partition_type = 1;
            
            MaterializedView mv = Mockito.mock(MaterializedView.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
            Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
            Mockito.when(partitionInfo.isUnPartitioned()).thenReturn(true);
            Mockito.when(partitionInfo.isRangePartition()).thenReturn(false);
            Mockito.when(partitionInfo.isListPartition()).thenReturn(false);
            Mockito.when(mv.getPartitionRefreshStrategy()).thenReturn(MaterializedView.PartitionRefreshStrategy.STRICT);
            
            Map<String, String> properties = new HashMap<>();
            MVRefreshParams params = new MVRefreshParams(mv, properties);
            
            // isNonTentativeForce should return true when isForce is true and isTentative is false
            Assertions.assertTrue(params.isNonTentativeForce(), 
                    "isNonTentativeForce should be true when config forces refresh and not tentative");
            
            // Set tentative to true
            params.setIsTentative(true);
            Assertions.assertFalse(params.isNonTentativeForce(), 
                    "isNonTentativeForce should be false when tentative is true");
            
        } finally {
            // Restore original config value
            Config.mv_refresh_force_partition_type = originalValue;
        }
    }
}
