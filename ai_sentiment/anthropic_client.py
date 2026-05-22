from anthropic import Anthropic
import json
import os

def analyze_titles(titles):
    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    user_prompt = (
        "Classify each headline into 'positive', 'negative', or 'neutral' and provide a confidence score "
        "between 0 and 1 for that label.\n"
        "Return a JSON array with objects in the same order as the headlines, each object with:\n"
        "- label: predicted sentiment\n"
        "- score: confidence (0.0 to 1.0)\n\n"
        "Headlines:\n"
    )
    for i, title in enumerate(titles, 1):
        user_prompt += f"{i}. {title}\n"

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system="You are a financial news analyzer.",
        messages=[
            {"role": "user", "content": user_prompt}
        ],
    )

    try:
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        data = json.loads(text.strip())
        return [
            {"label": item.get("label", "neutral"), "score": float(item.get("score", 0.5))}
            for item in data
        ]
    except (json.JSONDecodeError, IndexError, KeyError):
        return [{"label": "neutral", "score": 0.5} for _ in titles]
