from flask import Flask, request, jsonify
from transformers import pipeline

app = Flask(__name__)

finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert")
print("FinBERT ready")

@app.route("/analyze", methods=["POST"])
def analyze():
    data = request.get_json()
    text = data.get("text")

    if isinstance(text, str):
        text = [text]

    results = finbert(text)
    return jsonify(results)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)