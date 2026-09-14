# prefect-sample

A minimal but production-shaped [Prefect 3](https://docs.prefect.io) flow repository, used to test a self-hosted Prefect server with a Kubernetes work pool. Testing only, nothing here matters.

## Layout

| Path | Purpose |
|------|---------|
| `flows/url_check.py` | The flow: probes URLs concurrently with retrying tasks, fails the run on errors |
| `tests/` | Flow and task tests against Prefect's in-process test harness |
| `prefect.yaml` | Deployment definition: code source (git clone), schedule, parameters, pool sizing |
| `requirements.txt` | Runtime deps installed into the flow-run pod (empty; Prefect ships in the image) |
| `.github/workflows/ci.yml` | Lint, format check, tests, and a dry `prefect deploy` against an ephemeral server |

## Conventions

- **Code lives in git, not on the server.** The worker clones `main` at the start of every run. Deploying only registers metadata.
- **Prefect version pinned** to the one in the cluster's flow-run image, in `pyproject.toml` and `prefect.yaml`.
- **Infrastructure stays with the work pool.** Image, namespace, service account and security context are declared in the homelab repo's base job template; deployments only set sizing via `job_variables`.
- **Flows fail loudly.** A scheduled monitor that logs a warning and exits green is invisible; the flow raises when something is down.
- **Tests never need a server.** `prefect_test_harness` runs flows in-process against a temporary SQLite database.

## Local use

```bash
uv sync --group dev
uv run pytest -q
uv run python flows/url_check.py           # runs the flow locally, no server needed
```

## Registering on the server

The server API is only reachable inside the cluster network, so point the CLI at it through a port-forward:

```bash
kubectl -n prefect port-forward svc/prefect-server 4200:4200
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect deploy --all
```

The worker then picks up the schedule and runs each flow as a Kubernetes Job in the `prefect` namespace. Trigger an ad-hoc run with:

```bash
prefect deployment run 'url-check/url-check-hourly' --param timeout=5
```
