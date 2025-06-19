from pathlib import Path

import joblib
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors


MODEL_PATH = Path(__file__).resolve().parent / "trained_prompt_model.pkl"
VEC_PATH = Path(__file__).resolve().parent / "vectorizer.pkl"


def train_and_save_prompt_model(df: pd.DataFrame):
    if "query" not in df.columns or "prompt" not in df.columns:
        raise ValueError("CSV must contain 'query' and 'prompt' columns")

    queries = df["query"].astype(str).tolist()
    prompts = df["prompt"].astype(str).tolist()

    vectorizer = TfidfVectorizer(ngram_range=(1, 2), stop_words="english")
    X = vectorizer.fit_transform(queries)

    model = NearestNeighbors(n_neighbors=1, metric="cosine")
    model.fit(X)

    joblib.dump(model, MODEL_PATH)
    joblib.dump((vectorizer, prompts), VEC_PATH)
    print("✅ Prompt model and vectorizer saved.")
