# Thomas's Cookbook

A personal recipe collection app with a Botanical Apothecary aesthetic.

## Features

- Recipe CRUD with ingredients, steps, and images
- Triple ratings: Tastiness, Ease, Healthiness
- Cook Mode: full-screen step-by-step with wake lock
- Serving adjuster that recalculates ingredient quantities
- Integrated grocery list with per-recipe grouping
- Search, filter by meal type / cuisine / rating / cook time / dietary tags
- Auto-identify dishes from ingredients and instructions
- "I Made This" cook count tracking

## Quick Start

```bash
pip3 install -r requirements.txt
python3 server.py
```

Open [http://localhost:8742](http://localhost:8742) in your browser.

## Stack

- **Backend:** Python FastAPI + SQLite
- **Frontend:** Single-file React (served by the backend)
- **Database:** SQLite at `~/.cookbook/cookbook.db`

## API

Runs on port 8742. Set `COOKBOOK_DB_DIR` env var to change the database location.
