"""finance-mcp: read-only MCP tools over a Beancount ledger, for the kagent finance-curator agent.

Tool results hold personal financial data, so nothing here logs arguments, results or exception messages.

Environment:
  LEDGER_FILE                  default /bean/journal.beancount
  BASE_CURRENCY                default CHF
  EXCLUDE_TAG                  default UPCOMING (projected entries that have not happened yet)
  CASH_ROOT, CARD_ROOT         default Assets:Cash, Liabilities:CC
  STALE_PRICE_DAYS             default 30
  HOST, PORT                   default 0.0.0.0, 8000
  ALLOWED_HOSTS                comma-separated Host headers accepted on /mcp (DNS-rebinding protection)
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

from .ledger import Ledger

log = logging.getLogger("finance-mcp")

ALLOWED_HOSTS = [h.strip() for h in os.environ.get("ALLOWED_HOSTS", "localhost,127.0.0.1").split(",") if h.strip()]

mcp = FastMCP(
    "finance-mcp",
    instructions=(
        "Read-only figures from one person's Beancount ledger. Every number is already computed: quote results "
        "as given and never do your own arithmetic on them. Projected entries are excluded."
    ),
    host=os.environ.get("HOST", "0.0.0.0"),
    port=int(os.environ.get("PORT", "8000")),
    stateless_http=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=ALLOWED_HOSTS + [f"{h}:*" for h in ALLOWED_HOSTS],
    ),
)

_ledger: Ledger | None = None


def ledger() -> Ledger:
    global _ledger
    if _ledger is None:
        _ledger = Ledger(
            os.environ.get("LEDGER_FILE", "/bean/journal.beancount"),
            base=os.environ.get("BASE_CURRENCY", "CHF"),
            exclude_tag=os.environ.get("EXCLUDE_TAG", "UPCOMING"),
            cash_root=os.environ.get("CASH_ROOT", "Assets:Cash"),
            card_root=os.environ.get("CARD_ROOT", "Liabilities:CC"),
            stale_days=int(os.environ.get("STALE_PRICE_DAYS", "30")),
        )
    return _ledger


def answer(call: Callable[[], dict]) -> dict:
    """Run a query. Bad input goes back to the agent; other failures report only their type."""
    try:
        return call()
    except ValueError as e:
        return {"error": f"invalid input: {e}"}
    except Exception as e:
        log.warning("query failed: %s", type(e).__name__)
        return {"error": f"query failed: {type(e).__name__}"}


@mcp.tool()
def net_worth(as_of: str = "", depth: int = 2) -> dict:
    """Total net worth in the base currency, by account group, with any stale or missing prices.

    as_of: optional YYYY-MM-DD, default today. depth: account levels in each group, default 2.
    """
    return answer(lambda: ledger().net_worth(as_of or None, depth))


@mcp.tool()
def spend_summary(start: str = "", end: str = "", depth: int = 2, top: int = 15) -> dict:
    """Expenses, income, net saved, savings rate and a monthly breakdown for a period.

    start and end: optional YYYY-MM-DD, default this month so far. depth: account levels per group.
    top: how many expense groups to list.
    """
    return answer(lambda: ledger().spend_summary(start or None, end or None, depth, top))


@mcp.tool()
def tag_total(tag: str, depth: int = 2) -> dict:
    """Total spent on a tag such as a trip, by account group. Unknown tags return close matches."""
    return answer(lambda: ledger().tag_total(tag, depth))


@mcp.tool()
def cash_outlook(target: float | None = None, months: int = 6) -> dict:
    """Cash, card debt, money available after cards, average monthly saving, and months to reach a target.

    target: optional amount in the base currency. months: how many full past months to average, default 6.
    """
    return answer(lambda: ledger().cash_outlook(target, months))


@mcp.tool()
def ledger_freshness() -> dict:
    """When each account's ledger file was last updated and last balance-checked, plus any load errors."""
    return answer(lambda: ledger().ledger_freshness())


@mcp.custom_route("/healthz", methods=["GET"])
async def healthz(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "WARNING"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
