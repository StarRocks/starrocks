import pandas as pd
import xgboost as xgb
import utils

def evaluate_predict_result(y_pred, y_test):
    mape = utils.mean_absolute_percentage_error(y_test, y_pred)
    print(f"MAPE on test set: {mape:.3f}%")

def evaluate_model(dtest, y_true, model):
    y_pred = model.predict(dtest)
    evaluate_predict_result(y_pred, y_true)

args = utils.parse_args()
model_file = args.model_file
test_data_file = args.test_data
predict_value = args.predict_value

# Load the test dataset
test_data = pd.read_csv(test_data_file)
test_data = test_data[test_data[predict_value] != 0]
test_data = utils.onehot_encode_tables(test_data)

X_test_data, y_test_data = (test_data.filter(regex=utils.features_regex), test_data[predict_value])
y_test_data = utils.transform_predict(y_test_data, args.transform)
dtest = xgb.DMatrix(X_test_data, label=y_test_data)

# Evaluate the model
model = xgb.Booster()
model.load_model(args.model_file)
X_test_data = X_test_data.reindex(columns=model.feature_names, fill_value=0)

# Predict using the loaded model
utils.print_dataset_stats(test_data[predict_value])
X_dtest_data = xgb.DMatrix(X_test_data, label=y_test_data)
evaluate_model(X_dtest_data, y_test_data, model)
