#!/bin/bash

predict_value_list=(cpuCostNs memCostBytes scanRows)
dataset_list=(fe.features.0115.csv)
testset_list=(self none fe.features.0115.csv)
transform_list=(none log)
eval_metric_list=('mae' 'rmse') # or MAE

for predict_value in "${predict_value_list[@]}"
do
    for dataset in "${dataset_list[@]}"
    do
        for transform in "${transform_list[@]}"
        do
            for testset in "${testset_list[@]}"
            do
                for eval_metric in "${eval_metric_list[@]}"
                do

                echo "predict_value: $predict_value, dataset: $dataset, transform: $transform, testset: $testset, eval: ${eval_metric}" | tee -a predict.log
                python3 train.py \
                    --dataset $dataset \
                    --predict_value $predict_value \
                    --transform $transform \
                    --eval_metric ${eval_metric} \
                    --test_data $testset | tee -a predict.log

                python3 evaluate.py \
                    --dataset $dataset \
                    --predict_value $predict_value \
                    --transform $transform \
                    --eval_metric ${eval_metric} \
                    --test_data $testset | tee -a predict.log

                done
            done
        done
    done
done
