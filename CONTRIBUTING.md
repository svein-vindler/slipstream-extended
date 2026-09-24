# Contributing

Thanks for your interest in improving Slipstream Extended. Issues and pull requests are
welcome.

## Development setup

```bash
git clone https://github.com/<you>/slipstream-extended.git
cd slipstream-extended

# Python pipeline
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

# Cloudflare Worker
cd worker && npm ci && cd ..
```

## Run the checks

```bash
ruff check .                 # lint the Python pipeline
pytest                       # Python tests
( cd worker && npm run typecheck && npm test )  # generated types + tests
```

CI runs the same checks on every pull request.

Runtime and development Python inputs live in `requirements.in` and
`requirements-dev.in`. After deliberately changing a direct dependency,
regenerate both hash-locked `.txt` files with `pip-compile --generate-hashes`
under Python 3.12 or newer, then run `pip-audit -r requirements.txt
--disable-pip`. Do not hand-edit only the generated lock files.

## Project layout

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design. In short:
`pipeline/` fetches and writes the data, `worker/` serves it over MCP, and the
`.github/workflows/refresh.yml` Action runs the fetch on a schedule.

## Adding a data source

The pipeline is source-oriented. To add a source (for example Wahoo or Strava):

1. Add `pipeline/sources/<name>.py` exposing `fetch(...) -> list[Activity]`
   using the shared `Activity` model in `pipeline/schema.py`.
2. Call it from `pipeline/fetch.py`.
3. When combining multiple sources, add de-duplication (the same workout can
   appear in more than one place).
4. Add tests.

## Pull requests

- Keep changes focused and explain the "why" in the description.
- Add or update tests for behavior changes, and make sure `ruff`, `pytest`, and
  the Worker checks pass.
- **Never commit secrets.** Tokens, Access values, account identifiers, and personal data must
  stay out of the repo and out of issues/PRs.
- Be respectful and constructive in issues and pull requests.

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
