import os
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

client = OpenAI(
    api_key=os.getenv("GITHUB_TOKEN"),  # GitHub Models uses this
    base_url="https://models.inference.ai.azure.com"
)

def generate_summary(prompt: str):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a data analyst."},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content