from pathlib import Path
import joblib
import pandas as pd

from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(
    title="SLA Breach Prediction API",
    version="1.0.0",
    description="Predicts whether a payment transaction is likely to breach SLA."
)

MODEL_PATH = Path("data/models/model.pkl")

if not MODEL_PATH.exists():
    raise FileNotFoundError(f"Model file not found at {MODEL_PATH}")

model = joblib.load(MODEL_PATH)


class PredictionRequest(BaseModel):
    Message_Type: str
    Sender_Bank: str
    Receiver_Bank: str
    Service_Name: str
    Currency: str
    log_settlement_amount: float

    Source_Row_Count: int

    n_create_auth_events: int
    n_mod_auth_events: int
    n_auth_mod_events: int
    n_swift_auth_events: int
    n_swift_mod_events: int
    n_auth_swift_events: int
    n_available_timestamps: int

    had_modification: int
    had_auth_to_mod: int
    had_swift_modification: int
    had_rework: int

    n_creator_touchpoints: int
    n_modifier_touchpoints: int
    n_authorizer_touchpoints: int
    n_authorizer_to_mod_touchpoints: int
    n_total_operator_touchpoints: int
    n_unique_operators: int

    start_hour: int
    start_dayofweek: int
    start_month: int

    is_shift_1_hours: int
    is_shift_2_hours: int
    is_shift_overlap: int
    is_shift_hours: int
    is_outside_shift_hours: int


@app.get("/")
def root():
    return {
        "message": "SLA Breach Prediction API is running",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_loaded": True,
        "model_path": str(MODEL_PATH)
    }


@app.post("/predict")
def predict(request: PredictionRequest):
    input_dict = request.model_dump()
    input_df = pd.DataFrame([input_dict])

    expected_columns = model.feature_names_in_
    input_df = input_df.reindex(columns=expected_columns)

    predicted_class = int(model.predict(input_df)[0])
    predicted_probability = float(model.predict_proba(input_df)[0][1])

    return {
        "prediction": predicted_class,
        "breach_probability": round(predicted_probability, 6),
        "interpretation": "Likely SLA breach" if predicted_class == 1 else "Not likely to breach SLA"
    }


@app.post("/predict_batch")
def predict_batch(requests: list[PredictionRequest]):
    input_data = [r.model_dump() for r in requests]
    input_df = pd.DataFrame(input_data)

    expected_columns = model.feature_names_in_
    input_df = input_df.reindex(columns=expected_columns)

    predictions = model.predict(input_df)
    probabilities = model.predict_proba(input_df)[:, 1]

    results = []
    for pred, prob in zip(predictions, probabilities):
        results.append({
            "prediction": int(pred),
            "breach_probability": round(float(prob), 6),
            "interpretation": "Likely SLA breach" if int(pred) == 1 else "Not likely to breach SLA"
        })

    return {"results": results}