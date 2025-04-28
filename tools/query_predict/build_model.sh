#!/bin/bash
if [ ! -d "datas" ]; then
    mkdir -p datas
fi

if [ ! -f "datas/fe.features.log" ]; then
    echo "Please download the data first, and put it in the datas directory."
    exit 1
fi

python3 data_clean.py datas/fe.features.log datas/fe.features.csv
csvjson --stream datas/fe.features.csv > datas/data.json
python3 train.py --dataset datas/fe.features.csv --model_file datas/model.json
python3 evaluate.py --test_data datas/fe.features.csv --model_file datas/model.json

echo "Successfully prepare all data"