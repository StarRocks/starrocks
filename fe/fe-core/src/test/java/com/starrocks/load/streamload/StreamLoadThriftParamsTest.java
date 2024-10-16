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

package com.starrocks.load.streamload;

import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TStreamLoadPutRequest;
import org.junit.Test;

/** Tests for {@link StreamLoadThriftParams}. */
public class StreamLoadThriftParamsTest extends StreamLoadParamsTestBase {

    @Override
    protected StreamLoadParams buildFileFormatType(TFileFormatType expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setFormatType(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildFileType(TFileType expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setFileType(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildFilePath(String expected) {
        return new StreamLoadThriftParams(new TStreamLoadPutRequest());
    }

    @Override
    protected StreamLoadParams buildColumns(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setColumns(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildWhere(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setWhere(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildPartitions(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setPartitions(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildIsTempPartition(Boolean expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setIsTempPartition(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildNegative(Boolean expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setNegative(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Test
    @Override
    public void testMaxFilterRatio() {
        // thrift does not support max filter ratio
    }

    @Override
    protected StreamLoadParams buildMaxFilterRatio(Double expected) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected StreamLoadParams buildTimeout(Integer expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setTimeout(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildStrictMode(Boolean expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setStrictMode(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildTimezone(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setTimezone(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildLoadMemLimit(Long expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setLoadMemLimit(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildTransmissionCompressionType(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setTransmission_compression_type(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildLoadDop(Integer expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setLoad_dop(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildEnableReplicatedStorage(Boolean expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setEnable_replicated_storage(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildMergeCondition(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setMerge_condition(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildLogRejectedRecordNum(Long expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setLog_rejected_record_num(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildPartialUpdate(Boolean expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setPartial_update(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildPartialUpdateMode(TPartialUpdateMode expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setPartial_update_mode(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildPayloadCompressionType(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setPayload_compression_type(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildWarehouse(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setWarehouse(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildColumnSeparator(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setColumnSeparator(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildRowDelimiter(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setRowDelimiter(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildSkipHeader(Long expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setSkipHeader(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildEnclose(Byte expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setEnclose(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildEscape(Byte expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setEscape(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildTrimSpace(Boolean expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setTrimSpace(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildJsonPaths(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setJsonpaths(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildJsonRoot(String expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setJson_root(expected);
        }
        return new StreamLoadThriftParams(request);
    }

    @Override
    protected StreamLoadParams buildStripOuterArray(Boolean expected) {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        if (expected != null) {
            request.setStrip_outer_array(expected);
        }
        return new StreamLoadThriftParams(request);
    }
}
