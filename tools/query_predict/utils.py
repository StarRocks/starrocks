import argparse
import json
import numpy as np
import pandas as pd

# Convert bytes to human-readable format
def bytes_to_human_readable(bytes):
    if bytes < 1024:
        return f"{bytes} bytes"
    elif bytes < 1048576:
        return f"{bytes/1024:.2f} KB"
    elif bytes < 1073741824:
        return f"{bytes/1048576:.2f} MB"
    elif bytes < 1099511627776:
        return f"{bytes/1073741824:.2f} GB"
    else:
        return f"{bytes/1099511627776:.2f} TB"

def transform_predict(data, transform):
    if transform == 'log':
        return np.log1p(data)
    elif transform == 'shift':
        return np.right_shift(data, 20)
    elif transform == 'divide':
        return np.divide(data, (1 << 20))
    else:
        return data

def restore_predict(data, transform):
    if transform == 'log':
        return np.expm1(data)
    elif transform == 'shift':
        return np.left_shift(data, 20)
    elif transform == 'divide':
        return np.multiply(data, (1 << 20))
    else:
        return data

def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs((y_true - y_pred) / np.where(y_true != 0, y_true, 1))) * 100
    
# Output the min, max, and mean of 'memCostBytes' in dataset
def print_dataset_stats(y_test):
    stats = y_test.describe()
    print(stats)

def onehot_encode_tables(data): 
    data = pd.get_dummies(data, columns=data.filter(regex="^tables_").columns, prefix="tables")
    # deduplicate
    data = data.loc[:, ~data.columns.duplicated()]
    return data

def load_tables_encoding():
    with open(args.extra_file, "r") as file:
        table_columns = json.load(file)
    return [str(column) for column in table_columns]

def register_args(parser):
    parser.add_argument('--dataset', type=str, default='sql_features.csv', help="the dataset for training")
    parser.add_argument('--predict_value', type=str, default='memCostBytes', help='Value to predict')
    parser.add_argument('--eval_metric', type=str, default='mae', help='Evaluation metric')
    parser.add_argument('--transform', type=str, default='log', help='Transformation function')
    parser.add_argument('--test_data', type=str, default='none', help='Test data for prediction')
    parser.add_argument('--model_file', type=str, default='model.json', help='Model file for prediction')

def parse_args():
    parser = argparse.ArgumentParser( description="Commne arguments")
    register_args(parser)
    return parser.parse_args()

filter_regex = None

def get_filter_regex():
    global filter_regex
    if filter_regex is None:
        filter_regex = f"^(env_|var_|operators_|tables_|{args.predict_value})"
    return filter_regex
    
global args
features_regex = f"^(env_|var_|operators_|tables_)"
