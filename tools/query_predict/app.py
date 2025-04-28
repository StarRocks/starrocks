from fastapi import FastAPI, HTTPException, Request, Response
from typing import List, Dict
import pandas as pd
import io
import xgboost as xgb
import logging
import argparse
import utils
import json
import uvicorn
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class ModelReloadResponse(BaseModel):
    message: str

class PredictionResponse(BaseModel):
    predictions: List[float]

class ErrorResponse(BaseModel):
    error: str

def predict_data(input_data):
    # Common data processing steps for both CSV and JSON
    input_data = input_data.filter(regex=utils.features_regex)
    input_data = utils.onehot_encode_tables(input_data)
    input_data = input_data.reindex(columns=model.feature_names, fill_value=0)
    return xgb.DMatrix(input_data)  # Return DMatrix for prediction

def create_app(model_file_arg=None):
    app = FastAPI(title="XGBoost Prediction API")
    
    global model
    model = xgb.Booster()
    model.load_model(model_file_arg)

    @app.get("/health_check")
    async def health_check(request: Request):
        return Response(
            content="ok",
            media_type="text/plain"
        )
            
    @app.post("/predict_csv")
    async def predict_csv(request: Request):
        try:
            body = await request.body()
            csv_data = body.decode("utf-8").replace("\r\n", "\n")
            input_data = pd.read_csv(io.StringIO(csv_data))
            dinput = predict_data(input_data)

            predictions = model.predict(dinput)
            # Restore the predictions
            # The transform is hardcoded to "log" here as in utils#register_args. Adjust if needed.
            predictions = utils.restore_predict(predictions, transform="log")

            return Response(
                content="\n".join(map(str, predictions.tolist())),
                media_type="text/plain"
            )
        except Exception as e:
            logging.error(f"Error in predict_csv: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An error occurred during prediction.")

    @app.post("/predict_json", response_model=PredictionResponse)
    async def predict_json(request: Request):
        try:
            body = await request.body()
            json_data = body.decode("utf-8").strip().splitlines()
            input_data = pd.DataFrame([json.loads(line) for line in json_data])

            dinput = predict_data(input_data)
            predictions = model.predict(dinput)
            # Restore the predictions
            predictions = utils.restore_predict(predictions, transform="log")

            return PredictionResponse(predictions=predictions.tolist())
        except Exception as e:
            logging.error(f"Error in predict_json: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An error occurred during prediction.")

    @app.post("/reload_model", response_model=ModelReloadResponse)
    async def reload_model(request: Request):
        try:
            data = await request.body()
            temp_model = xgb.Booster()
            try:
                temp_model.load_model(bytearray(data))
                global model
                model = temp_model
                logging.info("Model reloaded from POST content.")
            except Exception as e:
                logging.error(f"Failed to load model from POST content: {e}")
                raise HTTPException(status_code=500, detail="Failed to load model")
            return ModelReloadResponse(message="Model reloaded successfully.")
        except Exception as e:
            logging.error(f"Error in reload_model: {e}")
            raise HTTPException(status_code=500, detail="An error occurred during model reloading.")

    @app.post("/reload_model_file", response_model=ModelReloadResponse)
    async def reload_model_file():
        try:
            model_file_path = model_file_arg
            temp_model = xgb.Booster()
            temp_model.load_model(model_file_path)
            global model
            model = temp_model
            logging.info("Model reloaded from file: %s", model_file_path)
            return ModelReloadResponse(message="Model reloaded successfully from file.")
        except Exception as e:
            logging.error(f"Error in reload_model_file: {e}")
            raise HTTPException(
                status_code=500,
                detail="An error occurred during model reloading from file."
            )

    return app

def parse_args():
    parser = argparse.ArgumentParser( description="Run the app with specified host and port.")
    utils.register_args(parser)
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host address (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=int, default=5000, help="Port number (default: 5000)"
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    app = create_app(args.model_file)

    logging.info(f"Starting server on {args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)
