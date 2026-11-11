# Bitfinex Exchange Logs → Coralogix

API poller that collects Bitfinex **account / exchange activity** for the last 5 minutes and ships it to Coralogix.

**Cadence:** every **5 minutes**, collect the **last 5 minutes** of activity, then POST to Coralogix.

```
Bitfinex API v2                          Coralogix Logs API
POST /v2/auth/r/movements/hist      ┐
POST /v2/auth/r/ledgers/hist        ┤ →  poller  →  POST /logs/v1/singles
POST /v2/auth/r/trades/hist (opt.)  ┘                   application: bitfinex
POST /v2/auth/r/orders/hist (opt.)                      subsystem: exchange-logs
```

Production mode is **cron every 5 minutes** with `--once`. Overlaps are safe because the poller checkpoints event IDs in `state.json`.

Bitfinex does not expose a single account-audit endpoint. This integration polls the official history APIs that cover deposits, withdrawals, wallet transfers, and optionally trades and closed orders.

Docs: [Introduction](https://docs.bitfinex.com/docs/introduction), [Authenticated REST](https://docs.bitfinex.com/docs/rest-auth).

## APIs used

| Direction | API | Auth |
| --- | --- | --- |
| Pull | Bitfinex `POST /v2/auth/r/movements/hist` (amount > 0 is a deposit) | HMAC-SHA384 (`bfx-apikey`, `bfx-nonce`, `bfx-signature`) |
| Pull | Bitfinex `POST /v2/auth/r/movements/hist` (amount < 0 is a withdrawal) | HMAC-SHA384 |
| Pull | Bitfinex `POST /v2/auth/r/ledgers/hist` (`category=51`, transfer) | HMAC-SHA384 |
| Pull | Bitfinex `POST /v2/auth/r/trades/hist` (optional) | HMAC-SHA384 |
| Pull | Bitfinex `POST /v2/auth/r/orders/hist` (optional, closed orders) | HMAC-SHA384 |
| Push | Coralogix `POST https://ingress.<domain>/logs/v1/singles` | Send-Your-Data API key (Bearer) |

Deposits and withdrawals are one movements feed. The poller splits them by the sign of `amount`. Wallet-to-wallet transfers are ledger category **51**. Deposit and withdrawal ledger categories are not polled, so those events are not ingested twice.

Bitfinex responses are positional arrays. The poller maps them to named fields from the v2 docs before shipping. Null placeholders are dropped. Any new non-null fields Bitfinex appends are kept on `unmapped`.

Authenticated calls are limited to **90 requests per minute**. A normal 5-minute run uses one request per source.

## Prerequisites

- Python 3.10+
- Bitfinex API key with **Read** only  
  Bitfinex → **API Keys** → Create New Key → enable **Read**  
  Do **not** enable Write, Trading, or Withdrawals  
  Enable Orders read only if you collect `trade` or `order`
- Use the key on this poller only. Each key has its own nonce, and the nonce must strictly increase
- Coralogix Send-Your-Data API key  
  Coralogix → **Data Flow → API Keys**
- Coralogix domain (`eu1.coralogix.com`, `coralogix.in`, …)

## Quick start

```bash
cd bitfinex-coralogix

python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
chmod +x run.sh
```

Edit `.env`:

```bash
BITFINEX_API_KEY=...
BITFINEX_API_SECRET=...
BITFINEX_BASE_URL=https://api.bitfinex.com
BITFINEX_SOURCES=deposit,withdraw,transfer

CORALOGIX_SEND_YOUR_DATA_KEY=...
CORALOGIX_DOMAIN=eu1.coralogix.com
CORALOGIX_APPLICATION_NAME=bitfinex
CORALOGIX_SUBSYSTEM_NAME=exchange-logs

POLL_INTERVAL_SECONDS=300
LOOKBACK_MINUTES=5
```

### Test one 5-minute window (no ingest)

```bash
python ship_bitfinex_to_coralogix.py --once --dry-run --lookback-minutes 5
```

### Test one 5-minute window (send to Coralogix)

```bash
python ship_bitfinex_to_coralogix.py --once --lookback-minutes 5
```

or:

```bash
./run.sh
```

You should see `Done. Total events shipped: N`. `N` can be `0` if the account had no deposit, withdraw, or transfer activity in the last 5 minutes.

### Optional in-process loop

```bash
python ship_bitfinex_to_coralogix.py
```

This sleeps `POLL_INTERVAL_SECONDS` (default `300`) between cycles. Prefer cron in production.

## Optional trade / order collection

Bitfinex trading symbols use a `t` prefix (`tBTCUSD`). A value without a `t` or `f` prefix is treated as a trading symbol.

```bash
BITFINEX_SOURCES=deposit,withdraw,transfer,trade,order
# Optional filter. Leave empty to collect all symbols.
BITFINEX_SYMBOLS=tBTCUSD,tETHUSD
```

Order history is closed and cancelled orders only, and Bitfinex keeps about **two weeks**. The time filter is order **creation** time (`MTS_CREATE`), which is what that endpoint supports.

To collect another ledger category beside transfers:

```bash
# 51 = transfer. See the category table on the ledgers docs.
BITFINEX_TRANSFER_CATEGORIES=51
```

## Cron every 5 minutes

### 1. Install on the host

```bash
sudo mkdir -p /opt/bitfinex-coralogix
sudo cp -a bitfinex-coralogix/. /opt/bitfinex-coralogix
cd /opt/bitfinex-coralogix

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
chmod +x run.sh
# put real API keys in .env
```

### 2. Confirm a manual run

```bash
/opt/bitfinex-coralogix/run.sh
```

You should see `Done. Total events shipped: N` (N may be `0` if nothing changed in the last 5 minutes).

### 3. Install crontab

```bash
crontab -e
```

Add:

```cron
# Bitfinex exchange logs → Coralogix (last 5 minutes, every 5 minutes)
*/5 * * * * /opt/bitfinex-coralogix/run.sh >> /var/log/bitfinex-coralogix.log 2>&1
```

`run.sh` already passes `--once --lookback-minutes 5`.

```bash
sudo touch /var/log/bitfinex-coralogix.log
sudo chown "$(whoami)" /var/log/bitfinex-coralogix.log
```

### 4. Verify cron

```bash
crontab -l
tail -f /var/log/bitfinex-coralogix.log
```

Wait until the next 5-minute boundary (`:00`, `:05`, `:10`, …).

### systemd timer alternative (Linux)

`/etc/systemd/system/bitfinex-coralogix.service`:

```ini
[Unit]
Description=Ship Bitfinex exchange logs to Coralogix

[Service]
Type=oneshot
WorkingDirectory=/opt/bitfinex-coralogix
EnvironmentFile=/opt/bitfinex-coralogix/.env
ExecStart=/opt/bitfinex-coralogix/.venv/bin/python ship_bitfinex_to_coralogix.py --once --lookback-minutes 5
```

`/etc/systemd/system/bitfinex-coralogix.timer`:

```ini
[Unit]
Description=Run Bitfinex → Coralogix every 5 minutes

[Timer]
OnCalendar=*:0/5
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bitfinex-coralogix.timer
```

## How the 5-minute window works

Each run asks Bitfinex for records between **now − 5 minutes** and **now**.

`state.json` stores recent event IDs and the last scanned timestamp per source. Missed cron runs catch up from the last checkpoint, including empty scans. A 2-second overlap covers clock skew. Movement status changes ship again because the event id includes the status and update time.

Bitfinex signs each request with the API secret. The nonce is a strictly increasing microsecond timestamp. Do not share the key with another bot.

## Finding logs in Coralogix

In **Explore** / **Logs**:

- Application: `bitfinex`
- Subsystem: `exchange-logs`

Useful fields inside `text`:

- `event_source`: `bitfinex_deposit`, `bitfinex_withdraw`, `bitfinex_transfer`, `bitfinex_trade`, `bitfinex_order`
- Deposit / withdraw: `currency`, `amount`, `direction`, `status`, `destinationAddress`, `transactionId`, `mtsUpdated`
- Transfer: `category` (`51`), `wallet`, `direction`, `balance`, `description`
- Trade / order: `symbol`, `side`, `execAmount` / `amount`, `orderId` / `id`

`description` is exchange text and can change. Filter transfers on `category`, not on that text.

## CLI

```text
python ship_bitfinex_to_coralogix.py --help

--once                  One cycle and exit (use with cron)
--dry-run               Fetch but do not send or save state
--lookback-minutes N    Override LOOKBACK_MINUTES (default 5)
--lookback-hours N      Convenience override
--log-level LEVEL       DEBUG, INFO, WARNING, ERROR
```

## Security

- Never commit `.env`. It is gitignored.
- Use a **Read-only** Bitfinex API key. Do not enable trading or withdrawals.
- Use the key on one client so nonces stay ordered.
- Rotate keys if they are exposed.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `nonce: small` / code `10114` | This key was already used with a higher nonce. Stop the other client, or create a new key for the poller. |
| `apikey: digest invalid` / code `10100` | Wrong key or secret, or the signature does not match the body. |
| Permission / auth error on one endpoint | Read permission for that resource is off. Orders and trades need Orders read. |
| `ERR_RATE_LIMIT` | More than 90 authenticated requests per minute. The poller sleeps 60 seconds and retries. |
| Looking for C2C / P2P history | Bitfinex has no official P2P order-history API, so this poller cannot collect it. |
| Orders older than about 2 weeks | `orders/hist` only returns recent closed or cancelled orders, filtered by creation time. |
| No logs in Coralogix | Wrong `CORALOGIX_DOMAIN`, or Explore time range misses event timestamps. |
| `Missing required environment variable` | `.env` missing in the cron working directory. |

## License

Use and modify freely for your organization.
