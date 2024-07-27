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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.THdfsProperties;

/**
 * Params for be push broker reader
 * TBrokerScanRange: file path and size, broker address
 * <p>
 * These params are sent to Be through push task
 */
public class SegmentBrokerReaderParams {
    TBrokerScanRange tBrokerScanRange;

    public SegmentBrokerReaderParams() {
        this.tBrokerScanRange = new TBrokerScanRange();
    }

    public void init(BrokerDesc brokerDesc) throws LoadException {
        // scan range params
        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        params.setStrict_mode(false);
        if (brokerDesc.hasBroker()) {
            params.setProperties(brokerDesc.getProperties());
            params.setUse_broker(true);
        } else {
            THdfsProperties hdfsProperties = new THdfsProperties();
            params.setHdfs_properties(hdfsProperties);
            params.setHdfs_read_buffer_size_kb(Config.hdfs_read_buffer_size_kb);
            params.setUse_broker(false);
        }

        tBrokerScanRange.setParams(params);

        // broker address updated for each replica
        tBrokerScanRange.setBroker_addresses(Lists.newArrayList());

        // broker range desc
        TBrokerRangeDesc tBrokerRangeDesc = new TBrokerRangeDesc();
        tBrokerRangeDesc.setFile_type(TFileType.FILE_BROKER);
        tBrokerRangeDesc.setFormat_type(TFileFormatType.FORMAT_STARROCKS);
        tBrokerRangeDesc.setSplittable(false);
        tBrokerRangeDesc.setStart_offset(0);
        tBrokerRangeDesc.setSize(-1);
        // path and file size updated for each replica
        tBrokerScanRange.setRanges(Lists.newArrayList(tBrokerRangeDesc));
    }
}
