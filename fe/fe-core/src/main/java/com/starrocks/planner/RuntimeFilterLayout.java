package com.starrocks.planner;

import com.starrocks.thrift.TJoinDistributionMode;
import com.starrocks.thrift.TRuntimeFilterLayout;
import com.starrocks.thrift.TRuntimeFilterLayoutMode;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

public class RuntimeFilterLayout {
    private int filterId;
    private TRuntimeFilterLayoutMode localMode;
    private TRuntimeFilterLayoutMode globalMode;
    private TJoinDistributionMode joinMode;
    private boolean pipelineLevelMultiPartitioned;
    private int numInstances;
    private int numDriversPerInstance;
    private List<Integer> bucketSeqToInstance;
    private List<Integer> bucketSeqToDriverSeq;
    private List<Integer> bucketSeqToPartition;

    public RuntimeFilterLayout(int filterId,
                               TJoinDistributionMode joinMode, boolean pipelineLevelMultiPartitioned, int numInstances,
                               int numDriversPerInstance, List<Integer> bucketSeqToInstance,
                               List<Integer> bucketSeqToDriverSeq, List<Integer> bucketSeqToPartition) {
        this.filterId = filterId;
        this.joinMode = joinMode;
        this.pipelineLevelMultiPartitioned = pipelineLevelMultiPartitioned;
        this.numInstances = numInstances;
        this.numDriversPerInstance = numDriversPerInstance;
        this.bucketSeqToInstance = bucketSeqToInstance;
        this.bucketSeqToDriverSeq = bucketSeqToDriverSeq;
        this.bucketSeqToPartition = bucketSeqToPartition;
    }

    public int getFilterId() {
        return filterId;
    }

    public TRuntimeFilterLayoutMode getLocalMode() {
        return localMode;
    }

    public TRuntimeFilterLayoutMode getGlobalMode() {
        return globalMode;
    }

    public TJoinDistributionMode getJoinMode() {
        return joinMode;
    }

    public boolean isPipelineLevelMultiPartitioned() {
        return pipelineLevelMultiPartitioned;
    }

    public int getNumInstances() {
        return numInstances;
    }

    public int getNumDriversPerInstance() {
        return numDriversPerInstance;
    }

    public List<Integer> getBucketSeqToInstance() {
        return bucketSeqToInstance;
    }

    public List<Integer> getBucketSeqToDriverSeq() {
        return bucketSeqToDriverSeq;
    }

    public List<Integer> getBucketSeqToPartition() {
        return bucketSeqToPartition;
    }

    public static RuntimeFilterLayout create(int filterId,
                                             TJoinDistributionMode joinMode, boolean pipelineLevelMultiPartitioned,
                                             int numInstances,
                                             int numDriversPerInstance, List<Integer> bucketSeqToInstance,
                                             List<Integer> bucketSeqToDriverSeq, List<Integer> bucketSeqToPartition) {
        RuntimeFilterLayout layout =
                new RuntimeFilterLayout(filterId, joinMode, pipelineLevelMultiPartitioned, numInstances,
                        numDriversPerInstance, bucketSeqToInstance, bucketSeqToDriverSeq, bucketSeqToPartition);
        layout.localMode = layout.computeLocalLayout();
        layout.globalMode = layout.computeGlobalLayout();
        return layout;
    }

    private TRuntimeFilterLayoutMode computeLocalLayout() {
        if (pipelineLevelMultiPartitioned) {
            if (joinMode == TJoinDistributionMode.BROADCAST || joinMode == TJoinDistributionMode.REPLICATED) {
                return TRuntimeFilterLayoutMode.SINGLETON;
            } else if (joinMode == TJoinDistributionMode.PARTITIONED ||
                    joinMode == TJoinDistributionMode.SHUFFLE_HASH_BUCKET) {
                return TRuntimeFilterLayoutMode.PIPELINE_SHUFFLE;
            } else if (joinMode == TJoinDistributionMode.COLOCATE ||
                    joinMode == TJoinDistributionMode.LOCAL_HASH_BUCKET) {
                return bucketSeqToDriverSeq.isEmpty() ?
                        TRuntimeFilterLayoutMode.PIPELINE_BUCKET_LX
                        : TRuntimeFilterLayoutMode.PIPELINE_BUCKET;
            } else {
                return TRuntimeFilterLayoutMode.NONE;
            }
        } else {
            return TRuntimeFilterLayoutMode.SINGLETON;
        }
    }

    private TRuntimeFilterLayoutMode computeGlobalLayout() {
        if (pipelineLevelMultiPartitioned) {
            if (joinMode == TJoinDistributionMode.BROADCAST || joinMode == TJoinDistributionMode.REPLICATED) {
                return TRuntimeFilterLayoutMode.SINGLETON;
            } else if (joinMode == TJoinDistributionMode.PARTITIONED ||
                    joinMode == TJoinDistributionMode.SHUFFLE_HASH_BUCKET) {
                return TRuntimeFilterLayoutMode.GLOBAL_SHUFFLE_2L;
            } else if (joinMode == TJoinDistributionMode.COLOCATE ||
                    joinMode == TJoinDistributionMode.LOCAL_HASH_BUCKET) {
                return (bucketSeqToDriverSeq == null || bucketSeqToDriverSeq.isEmpty()) ?
                        TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L_LX
                        : TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L;
            } else {
                return TRuntimeFilterLayoutMode.NONE;
            }
        } else {
            if (joinMode == TJoinDistributionMode.BROADCAST || joinMode == TJoinDistributionMode.REPLICATED) {
                return TRuntimeFilterLayoutMode.SINGLETON;
            } else if (joinMode == TJoinDistributionMode.PARTITIONED ||
                    joinMode == TJoinDistributionMode.SHUFFLE_HASH_BUCKET) {
                return TRuntimeFilterLayoutMode.GLOBAL_SHUFFLE_1L;
            } else if (joinMode == TJoinDistributionMode.COLOCATE ||
                    joinMode == TJoinDistributionMode.LOCAL_HASH_BUCKET) {
                return TRuntimeFilterLayoutMode.GLOBAL_BUCKET_1L;
            } else {
                return TRuntimeFilterLayoutMode.NONE;
            }
        }
    }

    TRuntimeFilterLayout toThrift() {
        TRuntimeFilterLayout layout = new TRuntimeFilterLayout();
        layout.setFilter_id(filterId);
        layout.setLocal_layout(localMode);
        layout.setGlobal_layout(globalMode);
        layout.setPipeline_level_multi_partitioned(pipelineLevelMultiPartitioned);
        layout.setNum_instances(numInstances);
        layout.setNum_drivers_per_instance(numDriversPerInstance);
        if (CollectionUtils.isNotEmpty(bucketSeqToInstance)) {
            layout.setBucketseq_to_instance(bucketSeqToInstance);
        }
        if (CollectionUtils.isNotEmpty(bucketSeqToDriverSeq)) {
            layout.setBucketseq_to_driverseq(bucketSeqToDriverSeq);
        }
        if (CollectionUtils.isNotEmpty(bucketSeqToPartition)) {
            layout.setBucketseq_to_partition(bucketSeqToPartition);
        }
        return layout;
    }
}
