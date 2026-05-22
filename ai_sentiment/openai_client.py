from openai import OpenAI
import json
import os

def analyze_titles(titles):
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    analysis = []

    # Prepare batch prompt for multiple titles
    user_prompt = (
        "You are a financial news analyzer.\n\n"
        "Classify each headline into 'positive', 'negative', or 'neutral' and provide a confidence score "
        "between 0 and 1 for that label.\n"
        "Return a JSON array with objects in the same order as the headlines, each object with:\n"
        "- label: predicted sentiment\n"
        "- score: confidence (0.0 to 1.0)\n\n"
        "Headlines:\n"
    )
    for i, title in enumerate(titles, 1):
        user_prompt += f"{i}. {title}\n"

    # Make one API call for all titles
    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[
            {"role": "system", "content": "You are a financial news analyzer."},
            {"role": "user", "content": user_prompt}
        ],
        # Remove temperature=0 for GPT-5 Mini (unsupported)
    )

    # Parse GPT response
    try:
        # GPT should return a JSON array: [{"label":..., "score":...}, ...]
        data = json.loads(response.choices[0].message.content)
        for item in data:
            analysis.append({
                "label": item.get("label", "neutral"),
                "score": float(item.get("score", 0.5))
            })
    except json.JSONDecodeError:
        # fallback: neutral with 0.5 confidence
        for _ in titles:
            analysis.append({"label": "neutral", "score": 0.5})

    return analysis