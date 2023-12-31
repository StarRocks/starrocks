/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starrocks.data.load.stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class StreamLoadResponse
        implements Serializable
{
    private boolean cancel;
    private Long flushRows;
    private Long flushBytes;
    private Long costNanoTime;
    private StreamLoadResponseBody body;
    private Exception exception;

    public void cancel()
    {
        this.cancel = true;
    }

    public boolean isCancel()
    {
        return cancel;
    }

    public Long getFlushRows()
    {
        return flushRows;
    }

    public Long getFlushBytes()
    {
        return flushBytes;
    }

    public Long getCostNanoTime()
    {
        return costNanoTime;
    }

    public StreamLoadResponseBody getBody()
    {
        return body;
    }

    public Exception getException()
    {
        return exception;
    }

    public void setFlushBytes(long flushBytes)
    {
        this.flushBytes = flushBytes;
    }

    public void setFlushRows(long flushRows)
    {
        this.flushRows = flushRows;
    }

    public void setCostNanoTime(long costNanoTime)
    {
        this.costNanoTime = costNanoTime;
    }

    public void setBody(StreamLoadResponseBody body)
    {
        this.body = body;
    }

    public void setException(Exception e)
    {
        this.exception = e;
    }

    public static class StreamLoadResponseBody
            implements Serializable
    {
        @JsonProperty(value = "TxnId")
        private Long txnId;
        @JsonProperty(value = "Label")
        private String label;
        @JsonProperty(value = "State")
        private String state;
        @JsonProperty(value = "Status")
        private String status;
        @JsonProperty(value = "ExistingJobStatus")
        private String existingJobStatus;
        @JsonProperty(value = "Message")
        private String message;
        @JsonProperty(value = "Msg")
        private String msg;
        @JsonProperty(value = "NumberTotalRows")
        private Long numberTotalRows;
        @JsonProperty(value = "NumberLoadedRows")
        private Long numberLoadedRows;
        @JsonProperty(value = "NumberFilteredRows")
        private Long numberFilteredRows;
        @JsonProperty(value = "NumberUnselectedRows")
        private Long numberUnselectedRows;
        @JsonProperty(value = "ErrorURL")
        private String errorURL;
        @JsonProperty(value = "LoadBytes")
        private Long loadBytes;
        @JsonProperty(value = "LoadTimeMs")
        private Long loadTimeMs;
        @JsonProperty(value = "BeginTxnTimeMs")
        private Long beginTxnTimeMs;
        @JsonProperty(value = "StreamLoadPlanTimeMs")
        private Long streamLoadPutTimeMs;
        @JsonProperty(value = "ReadDataTimeMs")
        private Long readDataTimeMs;
        @JsonProperty(value = "WriteDataTimeMs")
        private Long writeDataTimeMs;
        @JsonProperty(value = "CommitAndPublishTimeMs")
        private Long commitAndPublishTimeMs;

        public Long getNumberTotalRows()
        {
            return numberTotalRows;
        }

        public Long getNumberLoadedRows()
        {
            return numberLoadedRows;
        }

        public void setTxnId(Long txnId)
        {
            this.txnId = txnId;
        }

        public void setLabel(String label)
        {
            this.label = label;
        }

        public void setState(String state)
        {
            this.state = state;
        }

        public void setStatus(String status)
        {
            this.status = status;
        }

        public void setExistingJobStatus(String existingJobStatus)
        {
            this.existingJobStatus = existingJobStatus;
        }

        public void setMessage(String message)
        {
            this.message = message;
        }

        public void setMsg(String msg)
        {
            this.msg = msg;
        }

        public void setNumberTotalRows(Long numberTotalRows)
        {
            this.numberTotalRows = numberTotalRows;
        }

        public void setNumberLoadedRows(Long numberLoadedRows)
        {
            this.numberLoadedRows = numberLoadedRows;
        }

        public void setNumberFilteredRows(Long numberFilteredRows)
        {
            this.numberFilteredRows = numberFilteredRows;
        }

        public void setNumberUnselectedRows(Long numberUnselectedRows)
        {
            this.numberUnselectedRows = numberUnselectedRows;
        }

        public void setErrorURL(String errorURL)
        {
            this.errorURL = errorURL;
        }

        public void setLoadBytes(Long loadBytes)
        {
            this.loadBytes = loadBytes;
        }

        public void setLoadTimeMs(Long loadTimeMs)
        {
            this.loadTimeMs = loadTimeMs;
        }

        public void setBeginTxnTimeMs(Long beginTxnTimeMs)
        {
            this.beginTxnTimeMs = beginTxnTimeMs;
        }

        public void setStreamLoadPutTimeMs(Long streamLoadPutTimeMs)
        {
            this.streamLoadPutTimeMs = streamLoadPutTimeMs;
        }

        public void setReadDataTimeMs(Long readDataTimeMs)
        {
            this.readDataTimeMs = readDataTimeMs;
        }

        public void setWriteDataTimeMs(Long writeDataTimeMs)
        {
            this.writeDataTimeMs = writeDataTimeMs;
        }

        public void setCommitAndPublishTimeMs(Long commitAndPublishTimeMs)
        {
            this.commitAndPublishTimeMs = commitAndPublishTimeMs;
        }

        public String getState()
        {
            return state;
        }

        public String getStatus()
        {
            return status;
        }

        public String getMsg()
        {
            return msg;
        }

        public Long getCommitAndPublishTimeMs()
        {
            return commitAndPublishTimeMs;
        }

        public Long getStreamLoadPutTimeMs()
        {
            return streamLoadPutTimeMs;
        }

        public Long getReadDataTimeMs()
        {
            return readDataTimeMs;
        }

        public Long getWriteDataTimeMs()
        {
            return writeDataTimeMs;
        }

        public Long getLoadTimeMs()
        {
            return loadTimeMs;
        }

        public Long getNumberFilteredRows()
        {
            return numberFilteredRows;
        }
    }
}
