# finance-mcp

Read-only MCP tools over a Beancount ledger, used by the kagent `finance-curator` agent. Every figure is computed in code, so the model only chooses a tool and reports the result.

| Tool | Answers |
| --- | --- |
| `net_worth` | Total in the base currency by account group, with stale or missing prices |
| `spend_summary` | Expenses, income, savings rate and a monthly breakdown for a period |
| `tag_total` | What a tag such as a trip cost; unknown tags return close matches |
| `cash_outlook` | Cash, card debt, money available, average saving, months to a target |
| `ledger_freshness` | When each ledger file was last updated and balance-checked |

The image holds no personal data. The ledger is mounted read-only at `LEDGER_FILE`, and configuration is listed at the top of `finance_mcp/server.py`.

Tool results are private, so the server never logs arguments, results or exception messages. `/mcp` and `/healthz` do not authenticate callers: restrict access with a NetworkPolicy.

## Tests

The tests use the made-up ledger in `tests/ledger`. Never add real data to it.

```bash
docker build -t finance-mcp:test .
docker run --rm --user 0 -v "$PWD/tests:/app/tests:ro" --entrypoint sh finance-mcp:test \
  -c 'pip install -q pytest && python -m pytest -q -p no:cacheprovider tests'
```
