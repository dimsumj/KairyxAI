**Why KairyxAI Exists**

As a Product Manager in the mobile gaming industry for over 7-8 years, I have been feeling the pain regularly regarding the disconnection between data analyze and marketing/retargeting efforts. 

It has been a messy process for each of the team members that they have to do manual process regularly (create rules, visualize data, make the decision, download data from one place, upload data to another place, execute the decisions etc etc). A lot of the time the data gets swamped in tasks and not synced up across teams (marketing, live ops, CS folks, PMs, Engs) so the result is on and off. Teams have to do caliberations again and again to make sure the rule is setup correctly, the data is cleaned up properly and uploaded without any issue before do the property marketing efforts. It just take TOO MUCH Time!

I want to stitch the splited data from different sources again with the AI powered decision making system and automate the whole process (at least majority of it for now).


Modern game teams already have:

- MMPs (AppsFlyer, Adjust)

- Analytics (Firebase, Amplitude, Mixpanel)

- Messaging tools (Braze, SendGrid, push providers)

What they don’t have is a system that:

- Understands messy, inconsistent event data automatically

- Learns player behavior patterns without manual segmentation

- Decides when not to message as much as when to engage

- Continuously optimizes for retention and monetization outcomes


_KairyxAI is built to be that AI decision layer._


**What KairyxAI Does**

At a high level, KairyxAI:

1. Connects to existing data sources via APIs and webhooks

2. Automatically normalizes and tags events using AI

3. Builds player-level behavioral models in near real time

4. Decides the best action (or no action) for each player

5. Executes engagement via email, push notification, or in-game hooks

6. Learns from outcomes to improve future decisions

All without requiring teams to define funnels, segments, or rules upfront.


**Core Principles**

- MMP-agnostic – Bring your existing stack

- Decision-first – Analytics exist only to drive actions

- Respectful engagement – Fewer, smarter messages

- Continuous learning – Every action improves the system

- Explainable AI – Every decision has a reason


**System Architecture (Conceptual)**


        Data Sources (MMPs / Analytics / Game Events)
               
                ↓
        
        AI Data Normalization
              
                ↓
      
        Player Modeling & Embeddings
              
                ↓
        
        Decision & Optimization Engine
               
                ↓
    
        Execution (Email / Push / In-Game)
              
                ↓
          
        Outcome Feedback Loop

        

**What This Repository Is**

This repo focuses on:

- Core system modules and interfaces

- AI agent prompts and system instructions

- Data normalization and decision logic

- Reference implementations for connectors and execution

**Current Status**

🚧 Early-stage / active development

Initial focus:

- API-based data ingestion

- AI-driven event normalization

- Player modeling and segmentation

- Automated churn-prevention actions

**Who This Is For**

- Game engineers and data engineers

- Product managers working on growth / live ops

- AI engineers interested in applied decision systems

- Founders building tooling for games or consumer apps

**Disclaimer**

KairyxAI is an experimental project.

Expect rapid iteration, architectural changes, and evolving abstractions.

=========================================

Test locally (no full infra required):

### Fast start (recommended)
```bash
./run_local_demo.sh
```
This starts:
- Backend: `http://localhost:8000`
- Frontend: `http://localhost:5173`

Default mode is local mock data backend (`DATA_BACKEND_MODE=mock`).

### Manual start
1. Checkout the project
2. browse to `/backend/services/`
3. run: `pip install -r requirements.txt`
4. choose data backend mode:
   - Mock mode (dev/qa): `export DATA_BACKEND_MODE=mock`
   - GCP mode (prod-like): `export DATA_BACKEND_MODE=gcp`
5. for GCP mode, set:
   - `export BIGQUERY_PROJECT_ID=<your_project_id>`
   - `export GCS_BUCKET_NAME=<your_bucket_name>`
   - configure ADC (Application Default Credentials)
6. run backend: `uvicorn main_service:app --reload --host 0.0.0.0 --port 8000 --reload-dir ../../frontend`
7. run frontend in `/frontend`: `npm install && npm run dev`

Gemini API key is optional now: if unavailable, churn scoring/action uses local heuristic fallback mode.

Local reliability/security defaults added:
- Ingestion retry + backoff for external source pulls
- Dead-letter log for failed ingestion: `.cache/ingest_dlq.jsonl`
- Local audit log for connector changes: `.audit.log.jsonl`
- Local SQLite job persistence: `backend/services/.kairyx_local.db`

P1 execution adapters (local-friendly):
- `email` channel routes via SendGrid when configured, otherwise local simulation
- `braze` channel routes via Braze REST when configured, otherwise push simulation
- Configure Braze via `POST /configure-braze` with `{ api_key, rest_endpoint }`

