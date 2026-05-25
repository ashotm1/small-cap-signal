function jsonToLines(data) {
     const timingInfo = `Model: ${data.model}
Model processing time: ${data.timing.model_time}s
Total processing time: ${data.timing.total_time}s
-----------------------------\n`;
    const articleLines = data.articles.map(a =>
`Ticker: ${a.ticker}
Title: ${a.title}
URL: ${a.url}
Tags: ${a.tags.join(", ")}
Sentiment: ${a.sentiment}
Score: ${a.score.toFixed(2)}
-----------------------------`).join("\n");
    return timingInfo + articleLines;
}



async function run() {
    const limit = document.getElementById("limit").value;
    const model = document.getElementById("model").value;

    const res = await fetch("/run", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({ 
            limit: parseInt(limit),
            model: model
        })
    });

    const data = await res.json();
    document.getElementById("output").innerText = jsonToLines(data);
}