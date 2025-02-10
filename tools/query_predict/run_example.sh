#!/bin/bash


gunzip -fk example_data/fe.features.log.0115.gz
python3 data_clean.py example_data/fe.features.log.0115 example_data/fe.features.0115.csv
csvjson --stream example_data/fe.features.0115.csv > example_data/data.json
python3 train.py --dataset example_data/fe.features.0115.csv --model_file example_data/model.json
python3 evaluate.py --test_data example_data/fe.features.0115.csv --model_file example_data/model.json

python3 app.py --model_file example_data/model.json & 
echo $! > app_process_id.txt  # Record the process ID into a file
sleep 5 && echo "running the server"
head -n10 example_data/fe.features.0115.csv |  curl localhost:5000/predict_csv -X POST -H "Content-Type: text/csv" --data-binary @-
head -n10 example_data/data.json | curl  localhost:5000/predict_json -X POST -H "Content-Type: text/json" --data-binary @-
curl localhost:5000/reload_model_file -X POST
head -n10 example_data/fe.features.0115.csv |  curl localhost:5000/predict_csv -X POST -H "Content-Type: text/csv" --data-binary @-
head -n10 example_data/data.json | curl  localhost:5000/predict_json -X POST -H "Content-Type: text/json" --data-binary @-
pkill -F app_process_id.txt  # Kill the process by the recorded process ID
rm -f app_process_id.txt

echo "Successfully run all examples"