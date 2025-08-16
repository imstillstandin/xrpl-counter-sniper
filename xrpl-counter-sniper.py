"""
XRPL Counter-Sniper Prototype (v0.3, Full PnL Edition)

Enhancements:
- Tracks per-symbol positions with average cost basis.
- Realized and unrealized PnL logged to pnl.csv.
- Background MTM loop marks to market every 30s.
- DRY_RUN simulates fills; live mode assumes submitted prices.
"""
import asyncio
import csv
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple

from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.asyncio.transaction import safe_sign_and_autofill_transaction, send_reliable_submission
from xrpl.wallet import Wallet
from xrpl.models.transactions import TicketCreate, OfferCreate
from xrpl.models.requests import Subscribe, BookOffers, AccountObjects

getcontext().prec = 28

RIPPLED_URL = os.getenv("XRPL_WS", "wss://xrplcluster.com")
NETWORK_TIMEOUT_S = 20

LAUNCH_WINDOW_LEDGER_COUNT = 100
MIN_BURST_TX = 2
CROSS_POOL_REPEATS_THRESHOLD = 2

DRY_RUN = True
MICRO_BUY_XRP_DROPS = 100_000
SLIPPAGE_BPS = 300
TARGET_PROFIT_MULTIPLIER = Decimal("2.0")
FALLBACK_SELL_AFTER_S = 120
SELL_SIZE_FRACTION = Decimal("0.8")

HOT_WALLET_SEED_ENV = "XRPL_SEED"

TELEGRAM_WEBHOOK = os.getenv("TELEGRAM_WEBHOOK", "")

LOG_DIR = os.getenv("LOG_DIR", "./logs")
SNIPER_LOG_CSV = os.path.join(LOG_DIR, "sniper_activity.csv")
POOL_LOG_CSV = os.path.join(LOG_DIR, "pools.csv")
TRADE_LOG_CSV = os.path.join(LOG_DIR, "trades.csv")
PNL_LOG_CSV = os.path.join(LOG_DIR, "pnl.csv")

PNL_MTM_INTERVAL_S = 30

os.makedirs(LOG_DIR, exist_ok=True)

@dataclass
class Pool:
    amm_create_tx: dict
    created_ledger: int
    created_at: float
    assets: Tuple[dict, dict]
    id_hint: str = field(default_factory=str)

@dataclass
class AccountBurst:
    account: str
    first_seen_ledger: int
    pool_ids: List[str] = field(default_factory=list)
    tx_count_early: int = 0
    score: float = 0.0

@dataclass
class Position:
    symbol: str
    qty_iou: Decimal = Decimal(0)
    avg_cost_xrp_per_iou: Decimal = Decimal(0)
    realized_pnl_xrp: Decimal = Decimal(0)

    def on_buy(self, qty: Decimal, px: Decimal):
        if qty <= 0:
            return
        cost = qty * px
        new_qty = self.qty_iou + qty
        if new_qty > 0:
            self.avg_cost_xrp_per_iou = ((self.qty_iou * self.avg_cost_xrp_per_iou) + cost) / new_qty
        self.qty_iou = new_qty

    def on_sell(self, qty: Decimal, px: Decimal):
        if qty <= 0 or self.qty_iou <= 0:
            return
        qty = min(qty, self.qty_iou)
        proceeds = qty * px
        cost = qty * self.avg_cost_xrp_per_iou
        self.realized_pnl_xrp += (proceeds - cost)
        self.qty_iou -= qty
        if self.qty_iou == 0:
            self.avg_cost_xrp_per_iou = Decimal(0)


def ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_csv(path: str, row: List):
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["ts", *[f"col{i}" for i in range(len(row))]])
        w.writerow([ts(), *row])

def write_pnl(symbol: str, pos: Position, mtm_px: Optional[Decimal]=None):
    unreal = None
    if mtm_px is not None and pos.qty_iou > 0:
        unreal = pos.qty_iou * (mtm_px - pos.avg_cost_xrp_per_iou)
    write_csv(PNL_LOG_CSV, [symbol, str(pos.qty_iou), str(pos.avg_cost_xrp_per_iou),
                            str(mtm_px) if mtm_px else "", str(unreal) if unreal else "", str(pos.realized_pnl_xrp)])

async def alert(msg: str):
    if not TELEGRAM_WEBHOOK:
        return
    try:
        import aiohttp
    except Exception:
        return
    try:
        async with aiohttp.ClientSession() as sess:
            await sess.get(TELEGRAM_WEBHOOK + ("&text=" + msg.replace(" ", "+")))
    except Exception:
        pass


def is_amm_create(tx: dict) -> bool:
    return tx.get("TransactionType") == "AMMCreate" and tx.get("validated", False)


def pool_assets_from_amm_create(tx: dict) -> Optional[Tuple[dict, dict]]:
    a1 = tx.get("Asset") or {}
    a2 = tx.get("Asset2") or {}
    if a1 and a2:
        return a1, a2
    return None


def is_xrp_asset(asset: dict) -> bool:
    return (isinstance(asset, dict) and asset.get("currency") == "XRP") or asset == "XRP"


def pool_key(a1: dict, a2: dict) -> str:
    def norm(a: dict) -> str:
        if is_xrp_asset(a):
            return "XRP"
        ccy = a.get("currency") or "?"
        iss = a.get("issuer") or "?"
        return f"{ccy}.{iss}"
    return "|".join(sorted([norm(a1), norm(a2)]))


def tx_is_tradey(tx: dict) -> bool:
    t = tx.get("TransactionType")
    if t in ("OfferCreate", "AMMBid", "Payment"):
        return True
    if t in ("AMMDeposit",):
        return True
    return False


async def best_price_xrp_per_iou(client: AsyncWebsocketClient, iou: dict) -> Optional[Decimal]:
    req = BookOffers(
        taker_gets={"currency": iou["currency"], "issuer": iou["issuer"]},
        taker_pays={"currency": "XRP"},
        limit=5,
    )
    resp = await client.request(req)
    offers = (resp.result or {}).get("offers", [])
    if not offers:
        return None
    q = Decimal(offers[0]["quality"])
    return q / Decimal(1_000_000)


def xrp_drops_to_xrp(drops: int) -> Decimal:
    return Decimal(drops) / Decimal(1_000_000)


async def get_one_ticket_sequence(client: AsyncWebsocketClient, address: str) -> Optional[int]:
    resp = await client.request(AccountObjects(account=address, type="ticket", limit=5))
    for obj in (resp.result or {}).get("account_objects", []):
        if obj.get("LedgerEntryType") == "Ticket":
            return int(obj.get("TicketSequence"))
    return None


class CounterSniper:
    def __init__(self, url: str):
        self.url = url
        self.client: Optional[AsyncWebsocketClient] = None
        self.tracked_pools: Dict[str, Pool] = {}
        self.account_bursts: Dict[str, AccountBurst] = {}
        self.current_ledger_index: int = 0
        self.wallet: Optional[Wallet] = None
        self.positions: Dict[str, Position] = {}

    def _symbol_from_iou(self, iou: dict) -> str:
        return f"{iou['currency']}.{iou['issuer']}"

    async def connect(self):
        self.client = AsyncWebsocketClient(self.url, timeout=NETWORK_TIMEOUT_S)
        await self.client.open()
        sub = Subscribe(streams=["transactions", "ledger"])
        await self.client.request(sub)
        print(f"[{ts()}] Connected & subscribed")

    async def run(self):
        if not self.client:
            await self.connect()
        assert self.client is not None
        async for msg in self.client:
            if not isinstance(msg, dict):
                continue
            if msg.get("type") == "ledgerClosed":
                self.current_ledger_index = msg.get("ledger_index", 0)
                continue
            if msg.get("type") != "transaction":
                continue

            tx = msg.get("transaction", {})
            validated = msg.get("validated", False)
            ledger_index = msg.get("ledger_index", 0)
            if not validated:
                continue

            if is_amm_create({**tx, "validated": validated}):
                assets = pool_assets_from_amm_create(tx)
                if not assets:
                    continue
                key = pool_key(*assets)
                pool = Pool(
                    amm_create_tx=tx,
                    created_ledger=ledger_index,
                    created_at=time.time(),
                    assets=assets,
                    id_hint=key,
                )
                self.tracked_pools[key] = pool
                print(f"[{ts()}] New pool: {key} @ ledger {ledger_index}")
                write_csv(POOL_LOG_CSV, [key, ledger_index, assets])
                await alert(f"New pool detected: {key}")
                continue

            if not self.tracked_pools:
                continue
            account = tx.get("Account")
            if not account:
                continue

            for key, pool in list(self.tracked_pools.items()):
                if ledger_index - pool.created_ledger > LAUNCH_WINDOW_LEDGER_COUNT:
                    continue
                if not tx_is_tradey(tx):
                    continue

                burst = self.account_bursts.get(account)
                if not burst:
                    burst = AccountBurst(account=account, first_seen_ledger=ledger_index)
                    self.account_bursts[account] = burst
                burst.tx_count_early += 1
                if key not in burst.pool_ids:
                    burst.pool_ids.append(key)
                earliness = max(LAUNCH_WINDOW_LEDGER_COUNT - (ledger_index - pool.created_ledger), 1)
                burst.score += 1.0 + (earliness / LAUNCH_WINDOW_LEDGER_COUNT)

                write_csv(SNIPER_LOG_CSV, [account, key, ledger_index, tx.get("TransactionType"), burst.tx_count_early, round(burst.score, 3)])

                if self._looks_like_sniper(burst):
                    await alert(f"Sniper {account[:6]}… detected on {key}")
                    await self._maybe_execute_anchor_cycle(pool, account)

    def _looks_like_sniper(self, burst: AccountBurst) -> bool:
        if burst.tx_count_early >= MIN_BURST_TX:
            return True
        if len(burst.pool_ids) >= CROSS_POOL_REPEATS_THRESHOLD:
            return True
        if burst.score >= 4.0:
            return True
        return False

    async def _maybe_execute_anchor_cycle(self, pool: Pool, sniper_acct: str):
        a1, a2 = pool.assets
        if is_xrp_asset(a1) and not is_xrp_asset(a2):
            iou = {"currency": a2["currency"], "issuer": a2["issuer"]}
        elif is_xrp_asset(a2) and not is_xrp_asset(a1):
            iou = {"currency": a1["currency"], "issuer": a1["issuer"]}
        else:
            return

        symbol = self._symbol_from_iou(iou)
        price = await best_price_xrp_per_iou(self.client, iou)
        if price is None:
            return

        xrp_to_spend = xrp_drops_to_xrp(MICRO_BUY_XRP_DROPS)
        max_price = price * (Decimal(1) + Decimal(SLIPPAGE_BPS) / Decimal(10_000))
        iou_to_receive = (xrp_to_spend / max_price)

        if DRY_RUN:
            write_csv(TRADE_LOG_CSV, ["DRY_RUN_BUY", pool.id_hint, sniper_acct, str(MICRO_BUY_XRP_DROPS), iou])
            pos = self.positions.get(symbol) or Position(symbol)
            pos.on_buy(iou_to_receive, max_price)
            self.positions[symbol] = pos
            return

        if not self.wallet:
            seed = os.getenv(HOT_WALLET_SEED_ENV)
            if not seed:
                return
            self.wallet = Wallet.from_seed(seed)
        # Live trading logic placeholder
        pos = self.positions.get(symbol) or Position(symbol)
        pos.on_buy(iou_to_receive, max_price)
        self.positions[symbol] = pos

    async def mtm_loop(self):
        while True:
            try:
                for symbol, pos in self.positions.items():
                    if pos.qty_iou <= 0:
                        write_pnl(symbol, pos)
                        continue
                    ccy, issuer = symbol.split(".",1)
                    iou = {"currency": ccy, "issuer": issuer}
                    px = await best_price_xrp_per_iou(self.client, iou)
                    if px:
                        write_pnl(symbol, pos, px)
                    else:
                        write_pnl(symbol, pos)
            except Exception as e:
                write_csv(PNL_LOG_CSV, ["ERROR", str(e)])
            await asyncio.sleep(PNL_MTM_INTERVAL_S)


async def main():
    bot = CounterSniper(RIPPLED_URL)
    await bot.connect()
    asyncio.create_task(bot.mtm_loop())
    try:
        await bot.run()
    finally:
        if bot.client:
            await bot.client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down…")



