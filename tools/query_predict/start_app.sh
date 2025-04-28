#!/bin/bash
DIR=`pwd`
if [ ! -f "datas/model.json" ]; then
    echo "Model file not found. Training the model..."
    exit 1
fi
if [ ! -d "logs" ]; then
    mkdir -p logs
fi

nohup python3 ${DIR}/app.py --model_file datas/model.json >logs/query_predict.log 2>&1 & 
