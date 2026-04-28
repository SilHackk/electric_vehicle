from fastapi import FastAPI
import joblib
import pandas as pd

app = FastAPI()

model = joblib.load("ev_station_tree_model.pkl")

@app.post("/predict")
def predict(data: dict):
    df = pd.DataFrame([data])
    
    prediction = model.predict(df)[0]

    result = {
        "best_station": prediction,
        "cost": df[f"station_{prediction}_cost"].iloc[0],
        "duration": df[f"station_{prediction}_duration"].iloc[0],
        "free": int(df[f"station_{prediction}_free"].iloc[0])
    }

    return result