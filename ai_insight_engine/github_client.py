import os
import requests
from dotenv import load_dotenv
from pathlib import Path

# Always resolve .env relative to this script's location, going up to project root
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path, override=True)

print("Loading .env from:", dotenv_path)
print("File exists:", dotenv_path.exists())

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

API_URL = "https://models.github.ai/inference/chat/completions"

def call_github_model(prompt: str) -> str:
    if not GITHUB_TOKEN:
        raise Exception("Missing GITHUB_TOKEN")

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a financial analyst specializing in banking data pipelines. "
                    "When analyzing transactions: identify behavioral patterns, flag repeated transaction descriptions, "
                    "comment on data pipeline health (Spark vs Kafka convergence), and give actionable risk flags. "
                    "Be specific with numbers. Never make generic statements that could apply to any dataset."
                )
            },
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2  # lower = more factual, less generic
    }

    response = requests.post(API_URL, headers=headers, json=payload)

    if response.status_code != 200:
        raise Exception(f"GitHub Model Error: {response.text}")

    return response.json()["choices"][0]["message"]["content"]