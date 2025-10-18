# api-monitor-system
# Advanced API Response Monitor

A complete **API monitoring system** that:

- Checks API endpoints for response time, status, and content validation.
- Sends SLA & latency alerts via Slack, Email, Telegram, or Twilio SMS.
- Supports automatic healing (Auto-heal).
- Displays real-time metrics on a dashboard (FastAPI + WebSocket).
- Generates periodic reports (PNG / PDF).

---

## **File Structure**

. ├── core_engine.py         # Main loop for Controller & Agent ├── dashboard_server.py    # FastAPI dashboard ├── report_generator.py    # Report generation (PNG/PDF) ├── controller_config.json # Controller configuration ├── agent_config.json      # Agent configuration ├── agents.json            # Agent registry (auto-generated) ├── api_monitor.db         # SQLite database (auto-generated) └── reports/               # Generated report folder

---

## **Required Libraries**

```bash
pip install aiohttp aiosqlite jsonschema prophet scikit-learn matplotlib fastapi uvicorn reportlab requests

> Prophet and ReportLab are optional depending on report generation needs.




---

Configuration

Controller

controller_config.json:

endpoints: List of APIs to monitor.

alert: Slack, SMTP, Telegram, Twilio setup.

auto_heal: Auto-heal configuration.

Other settings: poll_interval_seconds, request_timeout, predict_window_minutes, etc.


Agent

agent_config.json:

controller_url: Controller URL to send reports.

region: Agent location or name.

endpoints: APIs monitored by this agent.



---

How to Run

1️⃣ Run Controller

python3 core_engine.py --mode controller --config controller_config.json

2️⃣ Run Agent

python3 core_engine.py --mode agent --config agent_config.json

> Multiple agents can connect to the same controller with different region values.



3️⃣ Run Dashboard

python3 dashboard_server.py

Open in browser: http://localhost:8080/

Real-time metrics are displayed via WebSocket.


4️⃣ Generate Reports

python3 report_generator.py <endpoint_url>

Reports (PNG / PDF) will be saved in the reports/ folder.



---

Running with Docker (Optional)

Using Docker, Controller, Agent, and Dashboard can be run with a single command.

Example:

docker-compose up --build


---

Uploading to GitHub

1. Initialize Git:



git init

2. Add files:



git add .

3. Commit:



git commit -m "Initial commit of API Monitor system"

4. Create a GitHub repository (e.g., advanced-api-monitor).


5. Add remote:



git remote add origin https://github.com/<username>/advanced-api-monitor.git

6. Push:



git branch -M main
git push -u origin main

> Your code will now be on GitHub.




---

Editing Configuration & Testing

Edit controller_config.json and agent_config.json to set API URLs, Slack/Webhook/Email/Telegram/Twilio details.

Test with a small API to ensure everything works.
