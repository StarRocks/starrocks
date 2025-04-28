#!/bin/bash

predict_value_list=(memCostBytes)
dataset_list=(datas/fe.features.csv)
# testset_list=(self none datas/fe.features.csv)
testset_list=(datas/fe.features.csv)
model_file='datas/model.json'
transform_list=(log)
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
                    --model_file $model_file \
                    --eval_metric ${eval_metric} \
                    --test_data $testset | tee -a predict.log

                python3 evaluate.py \
                    --dataset $dataset \
                    --predict_value $predict_value \
                    --transform $transform \
                    --eval_metric ${eval_metric} \
                    --model_file $model_file \
                    --test_data $testset | tee -a predict.log

                done
            done
        done
    done
done
