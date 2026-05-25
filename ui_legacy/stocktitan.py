import requests
from bs4 import BeautifulSoup

from ui_legacy.ai_sentiment.openai_client import analyze_titles as openai_analyze
from ui_legacy.ai_sentiment.anthropic_client import analyze_titles as anthropic_analyze

FINBERT_URL = "http://127.0.0.1:5000/analyze"


def analyze_sentiment(texts, model="finbert"):
    if model == "finbert":
        res = requests.post(FINBERT_URL, json={"text": texts, "model": model})
        return res.json()
    elif model == "gpt-5-mini":
        return openai_analyze(texts)
    elif model == "claude-haiku":
        return anthropic_analyze(texts)
    else:
        raise ValueError("Unsupported model")


def scrape(limit=10, model="finbert"):
    url = "https://www.stocktitan.net/news/live.html"
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    feed = soup.find("div", attrs={"role": "feed"})
    if not feed:
        return []

    box = feed.find("div", class_=lambda c: c and "d-flex py-2" in c)

    articles = []
    count = 0

    while box and count < limit:
        ticker_div = box.find("div", class_="news-list-tickers")
        ticker = ticker_div.find("span").get_text(strip=True) if ticker_div else "N/A"

        title_div = box.find("div", {"name": "title"})
        link = title_div.find("a") if title_div else None

        title = link.get_text(strip=True) if link else "No title"
        url_link = link["href"] if link else "N/A"

        tags_div = box.find("div", class_="news-list-tags")
        tags = [t.get_text(strip=True) for t in tags_div.find_all("span")] if tags_div else []

        articles.append({
            "ticker": ticker,
            "title": title,
            "url": url_link,
            "tags": tags
        })

        count += 1
        box = box.find_next("div", class_=lambda c: c and "d-flex py-2" in c)

    # sentiment batch call
    titles = [a["title"] for a in articles]
    sentiments = analyze_sentiment(titles, model)

    for i, a in enumerate(articles):
        s = sentiments[i]
        a["sentiment"] = s["label"]
        a["score"] = float(s["score"])

    return articles