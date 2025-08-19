"""
XRPL Counter-Sniper Prototype (v0.4, Visibility + Burst Detector)

What's new in this unified build:
- Works with xrpl-py v4.x (keeps your old helper names via shims)
- Websocket connect fix (no timeout= arg)
- Heartbeat CSV + console pings every 30s
- tx/minute counter so you can *see* stream volume
- AMMCreate logging as before
- OfferCreate "new IOU vs XRP" burst detector to catch launches before AMM pools
- Optional dummy writer + optional backfill for recent pools
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
# xrpl-py v4 API; we add shims to keep old helper names used below.
from xrpl.asyncio.transaction import autofill, sign, submit_and_wait
from xrpl.wallet import Wallet
from xrpl.models.transactions import TicketCreate, OfferCreate
from xrpl.models.requests import Subscribe, BookOffers, AccountObjects, Ledger

getcontext().prec = 28

# ---------------- Config ----------------
RIPPLED_URL = os.getenv("XRPL_WS", "wss://xrplcluster.com")
NETWORK_TIMEOUT_S = 20

# Sniper heuristics (original)
LAUNCH_WINDOW_LEDGER_COUNT = 100
MIN_BURST_TX = 2
CROSS_POOL_REPEATS_THRESHOLD = 2

# Trading
DRY_RUN = True
MICRO_BUY_XRP_DROPS = 100_000
SLIPPAGE_BPS = 300
TARGET_PROFIT_MULTIPLIER = Decimal("2.0")
FALLBACK_SELL_AFTER_S = 120
SELL_SIZE_FRACTION = Decimal("0.8")

HOT_WALLET_SEED_ENV = "XRPL_SEED"

# Logging
# Replace your current LOG_DIR line with these two lines:
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.getenv("LOG_DIR", os.path.join(BASE_DIR, "logs"))
SNIPER_LOG_CSV = os.path.join(LOG_DIR, "sniper_activity.csv")
POOL_LOG_CSV = os.path.join(LOG_DIR, "pools.csv")
TRADE_LOG_CSV = os.path.join(LOG_DIR, "trades.csv")
PNL_LOG_CSV = os.path.join(LOG_DIR, "pnl.csv")
HEARTBEAT_LOG = os.path.join(LOG_DIR, "heartbeat.csv")
PNL_MTM_INTERVAL_S = 30

# Debug / visibility
VERBOSE_COUNTS = True  # print tx/min once per minute

# OfferCreate burst detector for NEW IOUs vs XRP (secondary trigger)
NEW_IOU_WINDOW_LEDGERS = 50
OFFER_BURST_THRESHOLD = 12
OFFER_BURST_MIN_UNIQUE = 5

# Backfill: scan last N ledgers once at startup (set 0 to disable)
BACKFILL_LEDGER_COUNT = 0  # e.g., 2000 to scan a few hours back

os.makedirs(LOG_DIR, exist_ok=True)
# Touch log files so 'tail -f' works even before first event
for _p in (SNIPER_LOG_CSV, POOL_LOG_CSV, TRADE_LOG_CSV, PNL_LOG_CSV, HEARTBEAT_LOG):
    if not os.path.exists(_p):
        open(_p, "a").close()

# ---------------- Shims for xrpl-py v4 ----------------
async def safe_sign_and_autofill_transaction(tx, client, wallet):
    filled = await autofill(tx, client)
    signed = sign(filled, wallet)  # sign() is sync in v4
    return signed

async def send_reliable_submission(signed, client):
    return await submit_and_wait(signed, client)

# ---------------- Data Models ----------------
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

# ---------------- Utils ----------------
def ts() -> str:
    return datetime.now(timezone.utc).isoformat()

def write_csv(path: str, row: List):
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["ts", *[f"col{i}" for i in range(len(row))]])
        w.writerow([ts(), *row])

def write_pnl(symbol: str, pos: Position, mtm_px: Optional[Decimal] = None):
    unreal = None
    if mtm_px is not None and pos.qty_iou > 0:
        unreal = pos.qty_iou * (mtm_px - pos.avg_cost_xrp_per_iou)
    write_csv(
        PNL_LOG_CSV,
        [
            symbol,
            str(pos.qty_iou),
            str(pos.avg_cost_xrp_per_iou),
            "" if mtm_px is None else str(mtm_px),
            "" if unreal is None else str(unreal),
            str(pos.realized_pnl_xrp),
        ],
    )

async def alert(msg: str):
    webhook = os.getenv("TELEGRAM_WEBHOOK", "")
    if not webhook:
        return
    try:
        import aiohttp
        async with aiohttp.ClientSession() as sess:
            await sess.get(webhook + ("&text=" + msg.replace(" ", "+")))
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
    q = Decimal(offers[0]["quality"])  # drops per IOU unit
    return q / Decimal(1_000_000)

def xrp_drops_to_xrp(drops: int) -> Decimal:
    return Decimal(drops) / Decimal(1_000_000)

async def get_one_ticket_sequence(client: AsyncWebsocketClient, address: str) -> Optional[int]:
    resp = await client.request(AccountObjects(account=address, type="ticket", limit=5))
    for obj in (resp.result or {}).get("account_objects", []):
        if obj.get("LedgerEntryType") == "Ticket":
            return int(obj.get("TicketSequence"))
    return None

# Offer parser (detect IOU vs XRP)
def _extract_iou_vs_xrp_from_offer(tx: dict) -> Optional[Tuple[dict, bool]]:
    tg = tx.get("TakerGets")
    tp = tx.get("TakerPays")

    def is_xrp(obj):
        return (obj is None) or isinstance(obj, (int, str)) or (isinstance(obj, dict) and obj.get("currency") == "XRP")

    # dict legs are IOUs; scalar/int legs imply XRP (drops)
    if isinstance(tg, dict) and tg.get("currency") != "XRP" and is_xrp(tp):
        return ({"currency": tg.get("currency"), "issuer": tg.get("issuer")}, True)
    if isinstance(tp, dict) and tp.get("currency") != "XRP" and is_xrp(tg):
        return ({"currency": tp.get("currency"), "issuer": tp.get("issuer")}, True)
    return None

# ---------------- Core ----------------
class CounterSniper:
    def __init__(self, url: str):
        self.url = url
        self.client: Optional[AsyncWebsocketClient] = None
        self.tracked_pools: Dict[str, Pool] = {}
        self.account_bursts: Dict[str, AccountBurst] = {}
        self.current_ledger_index: int = 0
        self.wallet: Optional[Wallet] = None
        self.positions: Dict[str, Position] = {}

        # visibility counters
        self._tx_count_minute = 0
        self._last_minute = int(time.time() // 60)

        # OfferCreate burst state: (currency, issuer) -> {count, first_ledger, accounts:set}
        self._iou_offer_bursts: Dict[Tuple[str, str], Dict] = {}

    def _symbol_from_iou(self, iou: dict) -> str:
        return f"{iou['currency']}.{iou['issuer']}"

    async def connect(self):
        # v4 constructor: no timeout kwarg; wrap awaits with wait_for
        self.client = AsyncWebsocketClient(self.url)
        await asyncio.wait_for(self.client.open(), timeout=NETWORK_TIMEOUT_S)
        sub = Subscribe(streams=["transactions", "ledger"])
        await asyncio.wait_for(self.client.request(sub), timeout=NETWORK_TIMEOUT_S)
        print(f"[{ts()}] Connected & subscribed", flush=True)

    async def run(self):
        if not self.client:
            await self.connect()
        assert self.client is not None

        async for msg in self.client:
            # visibility: tx/min counter once per minute
            now_min = int(time.time() // 60)
            if now_min != self._last_minute:
                if VERBOSE_COUNTS:
                    print(f"[HB] tx/min={self._tx_count_minute}", flush=True)
                self._tx_count_minute = 0
                self._last_minute = now_min
            self._tx_count_minute += 1

            if not isinstance(msg, dict):
                continue

            if msg.get("type") == "ledgerClosed":
                self.current_ledger_index = msg.get("ledger_index", 0)
                print(f"[HB] ledger {self.current_ledger_index}", flush=True)
                continue

            if msg.get("type") != "transaction":
                continue

            tx = msg.get("transaction", {})
            validated = msg.get("validated", False)
            ledger_index = msg.get("ledger_index", 0)
            if not validated:
                continue

            # --- 1) OfferCreate burst detector for NEW IOUs vs XRP ---
            if tx.get("TransactionType") == "OfferCreate":
                ext = _extract_iou_vs_xrp_from_offer(tx)
                if ext:
                    iou, vs_xrp = ext
                    if vs_xrp and iou.get("currency") and iou.get("issuer"):
                        key = (iou["currency"], iou["issuer"])
                        bucket = self._iou_offer_bursts.get(key)
                        if not bucket:
                            bucket = {"count": 0, "first_ledger": ledger_index, "accounts": set()}
                            self._iou_offer_bursts[key] = bucket
                        # reset if window passed
                        if ledger_index - bucket["first_ledger"] > NEW_IOU_WINDOW_LEDGERS:
                            bucket = {"count": 0, "first_ledger": ledger_index, "accounts": set()}
                            self._iou_offer_bursts[key] = bucket

                        bucket["count"] += 1
                        acct = tx.get("Account")
                        if acct:
                            bucket["accounts"].add(acct)

                        if (
                            bucket["count"] >= OFFER_BURST_THRESHOLD
                            and len(bucket["accounts"]) >= OFFER_BURST_MIN_UNIQUE
                        ):
                            symbol = f"{key[0]}.{key[1]}"
                            pool_key_hint = f"BURST_{symbol}"
                            print(f"[BURST] New-IOU Offer burst: {symbol} "
                                  f"(offers={bucket['count']}, uniq={len(bucket['accounts'])}, "
                                  f"ledgers={ledger_index - bucket['first_ledger']})", flush=True)

                            # log "pool-like" event for dashboard consistency
                            write_csv(POOL_LOG_CSV, [pool_key_hint, ledger_index, {"Asset": "XRP"}, {"Asset2": iou}])

                            pseudo_pool = Pool(
                                amm_create_tx={"TransactionType": "OfferCreate"},
                                created_ledger=ledger_index,
                                created_at=time.time(),
                                assets=({"currency": "XRP"}, {"currency": iou["currency"], "issuer": iou["issuer"]}),
                                id_hint=pool_key_hint,
                            )
                            await self._maybe_execute_anchor_cycle(pseudo_pool, acct or "unknown")
                            self._iou_offer_bursts.pop(key, None)
            # --- end burst detector ---

            # --- 2) Regular AMMCreate tracking ---
            if is_amm_create({**tx, "validated": validated}):
                assets = pool_assets_from_amm_create(tx)
                if assets:
                    key = pool_key(*assets)
                    pool = Pool(
                        amm_create_tx=tx,
                        created_ledger=ledger_index,
                        created_at=time.time(),
                        assets=assets,
                        id_hint=key,
                    )
                    self.tracked_pools[key] = pool
                    print(f"[{ts()}] New pool: {key} @ ledger {ledger_index}", flush=True)
                    write_csv(POOL_LOG_CSV, [key, ledger_index, assets])
                    await alert(f"New pool detected: {key}")
                continue

            # --- 3) Early actor scoring on tracked pools ---
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

                write_csv(
                    SNIPER_LOG_CSV,
                    [account, key, ledger_index, tx.get("TransactionType"), burst.tx_count_early, round(burst.score, 3)],
                )

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

        buy_offer = OfferCreate(
            account=self.wallet.classic_address,
            taker_gets=str(MICRO_BUY_XRP_DROPS),  # pay XRP (drops)
            taker_pays={"currency": iou["currency"], "issuer": iou["issuer"], "value": f"{iou_to_receive:.12f}"},
            flags=0,
        )
        signed_buy = await safe_sign_and_autofill_transaction(buy_offer, self.client, self.wallet)
        await send_reliable_submission(signed_buy, self.client)

        pos = self.positions.get(symbol) or Position(symbol)
        pos.on_buy(iou_to_receive, max_price)
        self.positions[symbol] = pos

    async def mtm_loop(self):
        while True:
            # heartbeat CSV + console ping every cycle
            write_csv(HEARTBEAT_LOG, ["alive"])
            print("[HB] bot alive… waiting for pools/snipers", flush=True)
            try:
                for symbol, pos in list(self.positions.items()):
                    if pos.qty_iou <= 0:
                        write_pnl(symbol, pos)
                        continue
                    ccy, issuer = symbol.split(".", 1)
                    iou = {"currency": ccy, "issuer": issuer}
                    px = await best_price_xrp_per_iou(self.client, iou)
                    if px is not None:
                        write_pnl(symbol, pos, px)
                    else:
                        write_pnl(symbol, pos)
            except Exception as e:
                write_csv(PNL_LOG_CSV, ["ERROR", str(e)])
            await asyncio.sleep(PNL_MTM_INTERVAL_S)

# ---------------- Optional helpers ----------------
async def _write_dummy_logs_once():
    """Write one fake row to each CSV so the dashboard renders immediately."""
    fake_key = "DEBUG_POOL|XRP|FOO.rIssuer"
    fake_iou = {"currency": "FOO", "issuer": "rIssuer"}
    write_csv(POOL_LOG_CSV, [fake_key, 0, {"Asset": {"currency": "XRP"}, "Asset2": fake_iou}])
    write_csv(SNIPER_LOG_CSV, ["rFAKEACC", fake_key, 0, "OfferCreate", 3, 4.7])
    write_csv(TRADE_LOG_CSV, ["DRY_RUN_BUY", fake_key, "rFAKEACC", "100000", fake_iou])
    pos = Position("FOO.rIssuer")
    pos.on_buy(Decimal("123.45"), Decimal("0.05"))
    write_pnl("FOO.rIssuer", pos, Decimal("0.06"))

async def backfill_recent_pools(client: AsyncWebsocketClient, num_ledgers: int):
    """Scan recent ledgers for AMMCreate once at startup to populate pools.csv."""
    if num_ledgers <= 0:
        return
    info = await client.request(Ledger(ledger_index="validated"))
    latest = int(info.result["ledger_index"])
    start = max(1, latest - num_ledgers + 1)
    print(f"[BF] Scanning ledgers {start}..{latest} for AMMCreate", flush=True)
    for idx in range(start, latest + 1):
        resp = await client.request(Ledger(ledger_index=idx, transactions=True, expand=True))
        txs = (resp.result.get("ledger", {}) or {}).get("transactions", [])
        for tx in txs or []:
            if isinstance(tx, dict) and tx.get("TransactionType") == "AMMCreate":
                assets = pool_assets_from_amm_create(tx) or ({}, {})
                key = pool_key(*assets)
                write_csv(POOL_LOG_CSV, [key, idx, assets])
                print(f"[BF] Found AMMCreate at ledger {idx}: {key}", flush=True)

# ---------------- Entry point ----------------
async def main():
    bot = CounterSniper(RIPPLED_URL)
    await bot.connect()

    # Uncomment either/both while testing:
    # await _write_dummy_logs_once()
    # await backfill_recent_pools(bot.client, BACKFILL_LEDGER_COUNT)

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
