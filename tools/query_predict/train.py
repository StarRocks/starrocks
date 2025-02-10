import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
import time
import utils

args = utils.parse_args()
dataset_file = args.dataset
predict_value = args.predict_value
eval_metric = args.eval_metric
transform = args.transform
model_file = args.model_file

# Training params
train_data_hold_ratio = 0.8
evaluate_data_ratio = 0.2
num_rounds = 200

# Model params
params = {
    "objective": "reg:squarederror",
    "eval_metric": eval_metric,
    "eta": 0.1,
    "max_depth": 6,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "lambda": 10,
    "gamma": 2,
    "alpha": 1,
}

# Load the dataset
data = pd.read_csv(dataset_file)
data = data[data[predict_value] != 0]
data = utils.onehot_encode_tables(data)

# Data preprocessing for train data
X = data.filter(regex=utils.features_regex)
y = data[predict_value]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=evaluate_data_ratio, random_state=42
)
y_train = utils.transform_predict(y_train, transform)
y_test = utils.transform_predict(y_test, transform)
dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)

# Train the model
evals = [(dtrain, "train"), (dtest, "test")]
start_time = time.time()
model = xgb.train(params, dtrain, num_rounds, evals, early_stopping_rounds=10)
end_time = time.time()
training_time = end_time - start_time
print(f"Model training time: {training_time:.3f} seconds")

# Print the most significant features and their relevancy
feature_importance = model.get_fscore()
feature_importance = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
print("==========================================")
print("Most significant features and their relevancy:")
for feature, importance in feature_importance[:10]:  # Select top 10
    print(f"{feature}: {importance}")
print("==========================================")

# Save the model
if model_file:
    model.save_model(model_file)
    print(f"Finish training model, saved to file: {model_file}")
else:
    print("Model file path is empty, model not saved.")
