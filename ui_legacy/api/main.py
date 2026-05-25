import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from ui_legacy.stocktitan import scrape
import time

# Resolve static/template relative to ui_legacy/, so the app runs from any cwd.
_UI = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

app = FastAPI()

# Serve HTML + static files
app.mount("/static", StaticFiles(directory=os.path.join(_UI, "static")), name="static")

@app.get("/", response_class=HTMLResponse)
def home():
    with open(os.path.join(_UI, "template", "index.html"), "r", encoding="utf-8") as f:
        return f.read()

@app.post("/run")
async def run(request: Request):
    start_total = time.perf_counter()  # total time measurement

    body = await request.json()
    limit = body.get("limit", 10)
    model = body.get("model", "gpt-5-mini")

    # Measure model time inside scrape()
    start_model = time.perf_counter()
    articles = scrape(limit, model)  # scrape already calls analyze_sentiment
    end_model = time.perf_counter()

    end_total = time.perf_counter()

    return {
        "articles": articles,
        "timing": {
            "model_time": round(end_model - start_model, 7),
            "total_time": round(end_total - start_total, 7)
        },
        "model": model
    }