## Quick Start


Run the pipeline with an example data:
```
bash run_example.sh
```

## Scripts

**Pipeline**

1. `data_clean.py`: transform the fe.features.log into CSV
2. `train.py`: train the model with prodivded CSv file
3. `evaluate.py`: evaluate the testset with model
4. [Optional] `predict_service.py`: running the prediction service


**Command Arguments**

| Name | Meaning |  Default Value | 
| --- | --- | --- | 
| dataset | The training dataset | | 
| predicte_value | memCostBytes/cpuCostNs/scanRows | memCostBytes |
| model_file | The file to store the trained model| model.json |
| test_data | The test dataset used in evaluate.py| |
| transform | transform to the predicate_value | none |
| eval_metric | The eval metric of the model | mae | 

**`train.py`**

Purpose: train the model with specified dataset

Example:
```python
python3 train.py \
    --dataset $dataset \
    --predict_value [memCostBytes|cpuCostNs|scanRows] \
    --transform $transform \
    --eval_metric ${eval_metric} \
    --test_data $testset
```

**`evaluate.py`**

Purpose: evaluate the model with a testset


```python
python3 evaluate.py \
    --dataset $dataset \
    --predict_value [memCostBytes|cpuCostNs|scanRows] \
    --transform $transform \
    --eval_metric ${eval_metric} \
    --test_data $testset 
```

**`data_clean.py`**

Purpose: tranform the fe.features.log into a CSV file for training

```
python3 data_clean.py fe.features.log.0115 fe.features.0115.csv
```

**`run_predict.sh`**

Purpose: run a lot of evaluations with various parameters

## Prediction Service

**run the prediction service**
```bash
python3 app.py --model_file model.json
```

**online reload the model**
```bash
# Method 1: upload the model via HTTP
curl localhost:5000/reload_model -X POST -H "Content-Type: text/json" --data-binary @model.json

# Method 2: reload it from the file
curl localhost:5000/reload_model_file -X POST 
```

**use curl as client**
```bash
# JSON format
head -n10 data.json | curl  localhost:5000/predict_json -X POST -H "Content-Type: text/json" --data-binary @-

# CSV format
head -n3 fe.features.0115.csv |  curl localhost:5000/predict_csv -X POST -H "Content-Type: text/csv" --data-binary @-
```


## Dataset Generation
Generate tpcds queries:
```bash
./dsqgen \
    -DIRECTORY ../query_templates \
    -INPUT ../query_templates/templates.lst \
    -VERBOSE Y \
    -QUALIFY Y \
    -SCALE 1 \
    -DIALECT netezza \
    -OUTPUT_DIR /tmp \
    -STREAMS 10
```

