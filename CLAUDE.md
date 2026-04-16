# Project Context:

## Tech Stack
- **Language:** Python 3.12+
- **Domain:** Batch ETL / data engineering (Logistics Intelligence Pipeline)
- **Orchestration:** Apache Airflow 2.9
- **Warehouse:** PostgreSQL 16 with SQLAlchemy 2.0 (sync style — Airflow tasks are sync workers)
- **Processing:** pandas + pyarrow (parquet for bronze layer)
- **Validation:** Pydantic v2
- **Package Manager:** uv (uses `pyproject.toml` + `uv.lock`)
- **Testing:** pytest (+ pytest-asyncio available for any async helpers)
- **Linting/Formatting:** ruff

## Development Commands
- **Install dependencies:** `uv sync`
- **Add a dependency:** `uv add <pkg>` (use `uv add --dev <pkg>` for dev deps)
- **Run a script:** `uv run python -m src.extract.extract_orders`
- **Run tests:** `uv run pytest`
- **Coverage:** `uv run pytest --cov=src --cov-report=term-missing`
- **Lint:** `uv run ruff check .`
- **Format:** `uv run ruff format .`
- **Stack up:** `docker compose up -d --build`
- **Stack down:** `docker compose down`

## Coding Standards
- **Style:** Strictly follow PEP 8. Use `ruff` for linting and formatting.
- **Typing:** Mandatory static typing for all function signatures and class attributes.
- **Pydantic:** Use `model_dump()` instead of `.dict()` and v2-style validators.
- **Documentation:** Google-style docstrings for modules and public classes.

## Critical Rules
- **Prohibited:** Do not use `print()` for logs; use the standard `logging` module.
- **Prohibited:** Do not use mutable default arguments (e.g., `list=[]`).
- **Security:** Never include secrets or API keys in the code; use environment variables (loaded via `python-dotenv` or Airflow Variables/Connections).
- **Testing:** Maintain a minimum test coverage of 85%.

## Workflow & Collaboration Rules
- **IMPORTANT: Never make changes without asking.** Always propose a plan first and wait for explicit confirmation (e.g., "Ready to proceed?") before modifying any files.
- **Commit/Push Operations:** Don't include any reference to Claude in the commit messages.
- **Commit/Push Briefing:** Before executing any `git commit` or `git push` operation, you MUST provide a concise briefing that summarizes:
  - What was changed (key files and logic).
  - Why the changes were made.
  - Any potential side effects or risks.
- **Plan Mode:** Proactively use Plan Mode (Shift+Tab) for non-trivial tasks to ensure alignment before implementation.
- **Plan Mode:** Don't take any guess that is not obvious, don't hesitate to ask if anything relevant is unclear.

## Custom Commands
- `/briefing`: Generates a high-level briefing of the project state. To fulfill this:
  1. Scan the codebase and `CLAUDE.md`.
  2. List "Current Progress": What is implemented and working.
  3. List "Pending Tasks": What is missing or marked with `# TODO` in the code.
  4. Identify "Critical Blockers": Any bugs or configuration issues found.
  5. Suggest "Next Best Step": A specific recommendation on what to work on next.
