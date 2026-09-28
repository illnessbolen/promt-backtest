# Этап 0: разведка источников данных (@bosona)

Дата: 2026-09-28. Кошелёк (proxy wallet): `0xc2ad03f79ca3f3c17d8c7de2612ce0c89b7d40ed`.

Как помечена надёжность утверждений:

- **[live]**: проверено живым запросом из этого окружения; сырые ответы лежат в `docs/samples/stage0/`.
- **[docs]**: прочитано на docs.polymarket.com (страницы `*.md`, changelog, OpenAPI `data-api.polymarket.com/v2/openapi.json`).
- **[src]**: официальный код Polymarket: `Polymarket/ctf-exchange-v2`, `Polymarket/rs-clob-client` @ `dc8f1e5`. Живым запросом не проверено.
- **[web]**: сторонние статьи. Используется только там, где нет первоисточника.

## Итог в пяти пунктах

1. **Всю историю выкачать можно, и быстро, но только через Data API v2.**
   - Курсорная пагинация, до 1000 строк на страницу, потолка глубины нет.
   - Реальный объём ~545K сделок и ~700K строк активности, это ~700 страниц (~10 мин).
   - Data API v1 упирается в offset 5000 и **выключается 24.10.2026**.
2. **Роль maker/taker определяется без блокчейна.** `/v2/trades?taker_only=true` возвращает ровно его taker-fills, а разница с `taker_only=false` — это maker-fills.
   Комиссия считается по формуле. Проверено до цента против `entry_fees_usdc` и `fees_paid`.
   **Подтверждено on-chain:** за час 129/129 fills совпали с `OrderFilled`, включая роль (16 taker) и комиссии ($11.48232 против $11.48237 по формуле).
3. **Данные противоречат CLAUDE.md в трёх местах** (§7):
   - «89.6K сделок» — это 89 590 **рынков**, а сделок ~540K;
   - «$20.6M» — это 20.6M **shares**, а в USDC ~$10.4M;
   - первая сделка была 2 июня, а не в мае.
   PnL ~+$347K подтверждается.
4. **Он никогда не продаёт:** 0 SELL за всю историю. Выход идёт только через MERGE и REDEEM.
   В выборке за 27.09 около **91% fills maker** и в 48% рынков он покупал обе стороны.
   Это похоже на маркет-мейкинг, проверять будем на этапе 4.
5. **Правила разрешения 5m/15m/4h сменились 07.08 и 14.08.2026** (спот Chainlink на TWAP Chainlink).
   1h и daily резолвятся по свечам Binance. Историю нужно анализировать по режимам.

---

## 0. Сеть: что доступно из окружения

| Хост | Статус | Нужен для |
|---|---|---|
| `data-api.polymarket.com` | ✅ 200 (открыт) | сделки, активность, позиции, PnL |
| `docs.polymarket.com` | ✅ 200 (открыт) | документация |
| `gamma-api.polymarket.com` | ✅ 200 | метаданные рынков, правила резолва, strike/final |
| `clob.polymarket.com` (REST) | ✅ 200 | книга, цены, tick/fee |
| `data-api.binance.vision` | ✅ 200 | Binance spot klines 1s / aggTrades |
| `api.exchange.coinbase.com` | ✅ 200 | Coinbase BTC-USD (лучший прокси Chainlink, §4.4) |
| `polygon-bor-rpc.publicnode.com` | ✅ 200 (открыт) | on-chain сверка, `eth_getLogs` **≤ 10 000 блоков** за запрос (§2.4) |
| `polygon-rpc.com` | ⚠️ сеть открыта, но RPC отвечает `401 "API key disabled, reason: tenant disabled"` | анонимный доступ больше не работает |
| прочие Polygon RPC (`polygon.drpc.org`, `1rpc.io`, ankr), polygonscan | ❌ 403 от прокси | не нужны |
| `ws-live-data.polymarket.com`, `ws-subscriptions-clob.polymarket.com`, `data-stream.binance.vision` | ❌ 403 | понадобятся на этапе 3 |
| `api.binance.com` | ⚠️ 451 (гео-блок) | заменяется `data-api.binance.vision` |

On-chain источник работает через `polygon-bor-rpc.publicnode.com`. Для этапа 1 он **не обязателен** (§1.6), но даёт `logIndex`, размеры и лимит-цены его ордеров и независимую сверку (§2.4).

---

## 1. Data API ([live] + [docs])

### 1.1 v1 или v2

- v2 вышел 2026-09-04. **v1 выключается 2026-10-24** [docs `migrate/data-api-v1-to-v2`]. v1 заморожен, новые поля появляются только в v2.
- Отличия v2:
  - ответ в конверте `{data, pagination}`;
  - поля в `snake_case`;
  - курсор `pagination.next_cursor` вместо offset;
  - `condition` принимает до 20 id;
  - `/closed-positions` заменён на `/v2/positions?status=CLOSED`.
- **Этап 1 строим на v2.** v1 проверен только для полноты картины.

### 1.2 Реальные лимиты (проверено [live])

| Эндпоинт | limit | Глубина | Время-окно | Примечание |
|---|---|---|---|---|
| v1 `GET /activity` | ≤500 (больше молча режется до 500) | **offset ≤ 5000**, иначе `400 "max historical activity offset of 5000 exceeded"` | `start`/`end` | до 5 500 строк на окно; за одни сутки у него 6 138 строк |
| v1 `GET /trades` | ≤10 000 | offset ≤ 10 000, иначе `400 "max historical trades offset of 10000 exceeded"` | `start`/`end` работают | `takerOnly` по умолчанию `true` |
| v1 `GET /closed-positions` | ≤50 | — | — | |
| **v2 `GET /v2/activity`** | ≤1000 (1001 даёт `400`) | курсор; проверено 25 страниц / 25 000 строк без упора | `start`/`end` (сек, включительно; `start=1` = вся история) | 0.77 с на страницу |
| **v2 `GET /v2/trades`** | ≤1000 | курсор | `start`/`end` (только для `user`) | **`taker_only` по умолчанию `true`** |
| v2 `GET /v2/positions` | ≤1000 | курсор | `start`/`end` по `last_event_at` | `status=OPEN\|REDEEMABLE\|REDEEMABLE_LOST\|MERGEABLE\|CLOSED` |

Rate limits [docs `api-reference/rate-limits`]:

- v2: общий 800/10 с; `/v2/trades` 300/10 с; `/v2/activity` и `/v2/positions` 200/10 с;
- v1: общий 1000/10 с, `/trades` 200/10 с;
- Gamma: 4000/10 с (`/events` 500, `/markets` 300);
- CLOB: `/book` 1500/10 с;
- при превышении v2 отвечает `429` + `Retry-After`.

### 1.3 Типы активности и особенности ([live])

За всю историю встретились: `TRADE`, `MERGE`, `REDEEM`, `REWARD`, `MAKER_REBATE`, **`TAKER_REBATE`**.
Последнего типа нет в enum официального Rust-клиента, так что парсер должен принимать неизвестные типы.
`SPLIT`, `CONVERSION` и `YIELD` у него не встречались.

- **REDEEM** с 2026-08-10 пишется по строке на исход [docs changelog].
  Пример: одна tx дала `Up size 150 → usdcSize 150` и `Down size 50 → usdcSize 0`. Выплата равна сумме `usdcSize` по tx.
- v1 дополнительно отдаёт «пустые» REDEEM (`size 0`, `asset ""`, `outcomeIndex 999`), за сутки таких 349. **v2 их не отдаёт.** В остальном v1 и v2 совпали строка в строку (за 27.09).
- `MAKER_REBATE`, `TAKER_REBATE` и `REWARD` идут раз в день, без `conditionId`.
- `price` в TRADE может быть нецелым тиком (`0.0800000037`): это отношение сумм. `size` дробный, 6 знаков.

### 1.4 Строка сделки v2 (реальный пример, `data_v2_activity.json`)

```json
{"proxy_wallet": "0xc2ad…40ed", "timestamp": 1790619864, "condition_id": "0x4e0ba471…86f7",
 "type": "TRADE", "size": 84.115385, "usdc_size": 62.245385, "price": 0.7400000012,
 "transaction_hash": "0x0780d41f…4298", "token_id": "101717…68447", "side": "BUY",
 "outcome_index": 0, "outcome": "Up", "slug": "eth-updown-5m-1790619600", "event_slug": "eth-updown-5m-1790619600",
 "title": "Ethereum Up or Down - September 28, 2:20PM-2:25PM ET", "name": "bosona", "pseudonym": "Impolite-Sister"}
```

**Чего нет ни в v1, ни в v2:** `log_index`, явной роли, комиссии fill'а, времени точнее секунды.

### 1.5 Точность времени

- `timestamp` — это **время блока в секундах** [docs OpenAPI]. Совпадает с `block.timestamp` соответствующих tx [live, RPC].
- Блок Polygon — **ровно 1.5 с** (2 400 блоков за час 27.09 16–17 UTC).
- Порядок внутри блока в API не восстановить: нужен on-chain `logIndex`.
- Мс-времени решений @bosona **нет нигде**. В V2 ордер подписывается с полем `timestamp` (мс) [src], но в calldata `matchOrders` у всех его ордеров `timestamp = 0` [live].
  Другие участники поле почти всегда заполняют: 56 из 57 чужих ордеров в двух проверенных tx, медианный лаг «блок − ордер» 2.8 с и 8.5 с.
  Значит, латентность его бота напрямую не измерить. Только косвенно, по споту (гипотеза 3) и по live-трекеру (этап 3).

### 1.6 Роль maker/taker без блокчейна ([live])

`/v2/trades?user=…&taker_only=true` по документации отдаёт «each fill once, on its taker side».
Проверка на окне 27.09 16:00–17:00 UTC:

- `taker_only=false` дал 129 строк, это **то же мультимножество**, что `/v2/activity?type=TRADE` и v1 `/activity`;
- `taker_only=true` дал 16 строк, все входят в 129;
- значит, 113 строк — maker-fills.

За все сутки 27.09: 4 750 fills, из них **taker 444 (9.3%)** и **maker 4 306 (90.7%)**. По USDC доля taker 19.7%.

Роль = разность мультимножеств по ключу `(tx, token, side, size, price)`.

**On-chain подтверждение** [live, RPC]: `eth_getLogs(OrderFilled, topic2 = bosona)` за то же окно (блоки 94 546 440–94 548 839) дал 129 событий, из них 16 с `taker = адрес биржи`.
Мультимножества `(token, size)` совпали с API и для всех fills, и для taker-подмножества.

### 1.7 Дубликаты ([live])

За сутки нашлось 3 пары **полностью одинаковых строк** в одной tx, например `0xc12bff…: BUY 249 @ 0.97 Down` ×2.
Обе строки — maker-fills (в `taker_only=true` их 0), то есть это два реальных fill'а, а не повтор API.
On-chain это два отдельных `OrderFilled` (logIndex 607 и 611) от двух разных ордеров по 249 @ 0.97, оба исполнены целиком.

Поэтому ключ дедупликации без `log_index` обязан включать порядковый номер `seq` среди одинаковых строк tx.
Все строки tx имеют одинаковый `timestamp`, так что при окнах, целиком покрывающих секунду tx, `seq` стабилен.

### 1.8 Полезные агрегаты v2 ([live])

- `/v2/user-stats`:
  - `trades: 89590` — это **число различных рынков** (так в доке миграции);
  - `trade_count: 540004`;
  - `volume` 20.69M (shares), `volume_usdc` 10.40M;
  - разложение PnL: `trade_pnl`, `fees_paid`, `maker_rebate`, `taker_rebate`, `reward_income`…
- `/v2/user-volume?start&end` — объём и число сделок за UTC-дни. Так получена дневная статистика с 02.06: 544 939 сделок.
- `/v2/user-pnl?interval=all&fidelity=1d` — дневной ряд PnL со всеми компонентами. Нужен для сверки этапа 1.
- `/v2/positions` содержит `entry_fees_usdc`, `realized_pnl`, `status`.
- Также есть `/v2/resolutions` (состояние резолва) и `/v2/status` (свежесть данных; lag ~4 с).

---

## 2. Можно ли выкачать всю историю

### 2.1 Объём

- Первая сделка: **2026-06-02 21:34:39 UTC**. Профиль создан 2026-05-21.
- За 119 дней **544 939 fills** (~4 600 в день, стабильно с первой недели).
- Добавим ~120K строк REDEEM/MERGE (~1 000 в сутки по срезу 27.09): всего **~0.65–0.7M строк активности**.

### 2.2 Стратегия выгрузки

- **v2 `/v2/activity?user=…&start=1&limit=1000`**, проход по курсору: ~700 страниц × 0.77 с ≈ **10 мин**.
  Курсор привязан к фильтрам, поэтому фильтры нужно пересылать на каждой странице.
- Отдельно `/v2/trades?taker_only=true` за тот же период (~50K строк) даёт роль.
- **Идемпотентная докачка:** `start = max(ts) − 1 ч` (перекрытие), `INSERT OR IGNORE` по ключу. Курсор между запусками не храним.
- v1 не годится. При потолке offset 5000 сутки не помещаются в одно окно, понадобились бы часовые окна: ~3 000 окон и более 5 000 запросов. К тому же v1 скоро выключат.

### 2.3 Gamma ([live])

- `/events` и `/markets`: максимум 100 на страницу, offset больше ~2000 даёт `422`.
- Для глубоких выборок есть `/events/keyset` и `/markets/keyset` (`after_cursor`/`next_cursor`).
- `/series`: максимум 50 на страницу.
- **`/markets` по умолчанию `closed=false`** [docs changelog 2026-04-09]: без `closed=true` закрытые рынки молча не возвращаются.
- Пачка из 50 `condition_ids` за запрос работает.

### 2.4 Запасной и проверочный источник: on-chain `OrderFilled` (CLOB V2)

Статус: **проверено** [live] через `polygon-bor-rpc.publicnode.com`, декодированные примеры лежат в `chain_orderfilled_samples.json`.
Для этапа 1 не обязателен, потому что роль и комиссия уже есть из API. Полезен для трёх вещей:

- `log_index` (точный ключ и порядок внутри блока);
- **параметры его ордеров из calldata** `matchOrders` (selector `0x3c2b4399`): размер и лимит-цена ордера, частичные исполнения, `orderHash` для склейки fill'ов одного ордера;
- независимая сверка API.

**Контракты и событие.**

- CLOB V2 работает с **2026-04-28 ~11:00 UTC** [docs changelog], так что вся история @bosona на V2.
- Адреса [docs `resources/contracts`, «single source of truth»]:
  - CTF Exchange `0xE111180000d2663C0091e4f400237545B87B996B` (Up/Down, `negRisk=false`; все проверенные tx идут сюда);
  - Neg Risk CTF Exchange `0xe2222d279d744050d28e00520010520000310F59`;
  - Conditional Tokens `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`;
  - pUSD `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`;
  - CtfCollateralAdapter `0xAdA100Db00Ca00073811820692005400218FcE1f`. В README ctf-exchange-v2 указан другой адрес, `0xADa100874d00e3331D00F2007a9c336a65009718`: адаптер передеплоили, учитывать оба.
- Событие [src `ITrading.sol`]: `OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, uint8 side, uint256 tokenId, uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee, bytes32 builder, bytes32 metadata)`, topic0 `0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee`.
  Суммы в 6 знаках. BUY: `makerAmountFilled` = pUSD, `takerAmountFilled` = токены. SELL наоборот.
- Роль [src `Trading.sol`, подтверждено live]:
  - maker-ордер эмитит `OrderFilled(maker = его владелец, taker = владелец taker-ордера)`;
  - taker-ордер эмитит `OrderFilled(maker = владелец taker-ордера, taker = адрес биржи)` + `OrdersMatched`;
  - значит, фильтр `topic2 == bosona`: при `topic3 == биржа` он taker, иначе maker.
- Split/merge/redeem идут через адаптер. Своих событий нет, видны как ERC-1155 `TransferBatch` на CTF.

**Что показала проверка** (tx `0x63326de7…`, `0x0780d41f…`, `0xc12bff78…` и час 27.09 16–17 UTC):

- 129/129 fills совпали с API, включая роль. `block.timestamp` = `timestamp` API.
- `fee` его taker-fill'а `108 @ 0.36` равен `1.741820`, это ровно `108 × 0.07 × 0.36 × 0.64`. У maker-fill'ов `fee = 0`.
- Его ордера подписаны `signatureType = 3` (POLY_1271), `signer` = сам proxy wallet. `Order.timestamp = 0` (§1.5).
- Taker-ордер `BUY 123.55 @ 0.36` исполнился на 108 и прошёл 3 уровня.
  Среди них встречная **покупка** противоположного токена: матч типа MINT, то есть пара Up+Down создаётся из коллатерала.
- Maker-ордер `BUY 150 @ 0.74` исполнился на 84.12. «Дубликат» из API — это два разных ордера `249 @ 0.97`.

**Лимиты и стоимость.**

- `eth_getLogs` принимает не больше **10 000 блоков** за запрос (`-32701 exceed maximum block range`).
- Его история — это блоки ~87.8M…94.6M, ~6.8M блоков. Получается ~680 запросов по 1–2 с, то есть **~15–20 мин** на все его `OrderFilled`.
- Calldata нужна выборочно, по одному `eth_getTransactionByHash` на tx.
- Альтернативы в 2026: Goldsky pipelines, Dune, Allium [docs `resources/blockchain-data`]. Старый subgraph после V2 неполный [web].

---

## 3. Метаданные рынков через Gamma ([live])

### 3.1 Вселенная Up/Down

Активы: BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, ZEC. Таймфреймы: 5m, 15m, 4h, 1h, daily.
По открытым сериям на 28.09 у BTC/ETH/SOL/XRP/DOGE/BNB/HYPE есть все пять таймфреймов, у ZEC только 5m/15m/4h.

| Таймфрейм | slug | seriesSlug |
|---|---|---|
| 5m / 15m / 4h | `{btc,eth,sol,xrp,doge,bnb,hype,zec}-updown-{5m,15m,4h}-{unix_ts начала окна}` | `btc-up-or-down-15m` … |
| 1h | `{bitcoin,ethereum,solana,xrp,dogecoin,bnb,hype}-up-or-down-{month}-{day}-{year}-{h}{am,pm}-et` | `btc-up-or-down-hourly` … |
| daily | `{…}-up-or-down-on-{month}-{day}-{year}` (в 2025 без года) | `btc-up-or-down-daily` … |

Тег `up-or-down` включает и акции, индексы, FX (`feeType=finance_prices_fees`). Их отфильтровываем.

### 3.2 Как получить рынок

- По slug: `GET /events/slug/{slug}` или `/markets/slug/{slug}`.
- Пачкой по conditionId: `GET /markets?closed=true&condition_ids=…&condition_ids=…` (до 50).
- По token id: `GET /markets?closed=true&clob_token_ids=…`.
- CLOB `GET /markets/{conditionId}` возвращает `tokens[{token_id, outcome, price, winner}]`.

### 3.3 Ключевые поля (`gamma_event_btc_15m_resolved.json`)

```jsonc
"conditionId": "0xf6a64d2d…db41e",
"outcomes": "[\"Up\", \"Down\"]", "clobTokenIds": "[\"72701660…\", \"72842718…\"]",  // порядок совпадает
"eventStartTime": "2026-09-28T17:30:00Z", "endDate": "2026-09-28T17:45:00Z",        // окно
"acceptingOrdersTimestamp": "2026-09-27T17:37:50Z",   // торги открыты за ~24 ч ДО окна
"closedTime": "2026-09-28 17:46:27+00", "outcomePrices": "[\"1\", \"0\"]",            // итог
"orderMinSize": 5, "orderPriceMinTickSize": 0.001,     // tick динамический 0.01 ↔ 0.001
"feeType": "crypto_fees_v2", "feeSchedule": {"exponent": 1, "rate": 0.07, "takerOnly": true, "rebateRate": 0.2},
"cryptoMarketConfig": {"id": "btc-15m-twap-60", "asset": "btc", "duration": "15m", "twapEnabled": true, "twapLookbackSeconds": 60},
"eventMetadata": {"priceToBeat": 83884.316…, "finalPrice": …}   // на уровне event
```

---

## 4. Правила разрешения ([live] описания + сверка чисел; даты подтверждены [docs changelog])

### 4.1 Сводка

| Рынки | Период | Источник | Правило | Ничья |
|---|---|---|---|---|
| 5m, 15m, 4h | до **2026-08-07 00:00 UTC** | Chainlink `{asset}-usd` (спот) | цена в конце ≥ цены в начале, тогда Up | Up |
| 15m, 4h | с 2026-08-07 00:00 UTC | Chainlink `{asset}-usd-twap-60s-streams` | TWAP 60 с ≥ strike, тогда Up | Up |
| 5m | 2026-08-07 → 2026-08-14 00:00 UTC | Chainlink TWAP 30 с | то же | Up |
| 5m | с 2026-08-14 00:00 UTC | Chainlink TWAP 60 с | то же | Up |
| 1h | весь период | Binance `{ASSET}USDT` свеча 1h | close ≥ open, тогда Up | Up |
| daily | весь период | Binance 1m свеча 12:00 ET | close(сегодня) > close(вчера), тогда Up | **50/50** |

В TWAP-эпоху и strike, и итог берутся из TWAP-фида [docs changelog 2026-08-07].
`cryptoMarketConfigId`: `null` (до ~августа), затем `btc-15m`, `btc-15m-twap-60`, `btc-5m-twap-30`, `btc-5m-twap-60`.

### 4.2 Strike и итог

- `eventMetadata.priceToBeat` и `finalPrice`:
  - 1h: совпали с open и close 1h-свечи Binance до цента;
  - daily: совпали с close 1m-свечей 12:00 ET;
  - 5m/15m/4h: цепочка `priceToBeat(N) == finalPrice(N−1)` точная.
- Покрытие 96–100% окон. Пропуски восстанавливаются цепочкой и `outcomePrices`.
- `priceToBeat` появляется в Gamma только после резолва предыдущего окна (~1 мин после старта). Live-трекеру strike считать самим.
- Задержка резолва: 5m/15m ~20–90 с, 1h и daily ~12–13 мин.

### 4.3 Chainlink в реальном времени

- Legacy RTDS (`wss://ws-live-data.polymarket.com`, без авторизации): топики `crypto_prices_chainlink`, `crypto_prices_twap_sixty`, `crypto_prices_twap_thirty`.
  Помечены legacy, **удаление запланировано** [docs `migrate/rtds-to-polybolt`].
- Замена — PolyBolt `wss://ws-live-v2.polymarket.com/ws` (`price.crypto.twap`).
  Он **требует CLOB API credentials**, а их получают подписью ключом кошелька. Это конфликтует с ограничением «никаких ключей», **решение нужно на этапе 3**.
  Кроме того, `price.crypto` в PolyBolt идёт от Pyth, а не от Chainlink.
- Исторического тикового Chainlink публично нет.

### 4.4 Качество прокси (замер [live], BTC 15m, 12 окон)

| Прокси | Смещение от Chainlink strike | σ |
|---|---|---|
| Coinbase `BTC-USD` close 1m перед границей (до TWAP) | **−0.07 bps** | 0.63 bps |
| Binance `BTCUSDT` 1s перед границей (до TWAP, 05.08) | −8.54 bps | 0.50 bps |
| Binance 1s, среднее 60 с (TWAP, 28.09): BTC / ETH / SOL | −3.26 / −3.37 / −3.51 bps | ~0.6 bps |

Binance 1s можно использовать с **поправкой на базис USDT/USD**, но базис дрейфует, поэтому калибруем его по `priceToBeat`.

---

## 5. Комиссии ([docs] + сверка [live])

- **Формула** [docs `trading/fees`]: `fee = C × feeRate × p × (1 − p)` в USDC, округление до 5 знаков.
  Crypto: `feeRate 0.07`, **maker платит 0**, maker rebate 20% собранных taker-комиссий.
  Комиссия ставится при матчинге (в V2 ордер не содержит `feeRateBps`).

  | p | taker fee на 100 shares | % номинала |
  |---|---|---|
  | 0.50 | $1.75 | 3.5% |
  | 0.70 | $1.47 | 2.1% |
  | 0.90 | $0.63 | 0.7% |
  | 0.95 | $0.33 | 0.35% |
  | 0.99 | $0.07 | 0.07% |

- **Сверка с его данными** [live]:
  - `entry_fees_usdc` 40 последних закрытых позиций: 37/40 совпали с формулой по его taker-fills до 5-го знака, у позиций только с maker-fills ровно 0;
  - 3 расхождения — позиции, у которых часть базиса списана при MERGE;
  - дневные `fees_paid` за 27.09: $393.18 по `/v2/user-pnl` против $393.55 по формуле;
  - on-chain `OrderFilled.fee` его taker-fills за час 27.09 16–17 UTC: $11.48232 против $11.48237 по формуле (расхождение только в округлении 5-го знака).
- **Историческая ставка:** верхняя огибающая implied rate по позициям 04.06, 25.06, 13.07, 27.07 и 21.08 равна **ровно 0.0700**.
  Сторонняя статья про «0.072 → 0.07 в июле» [web] ни changelog'ом, ни данными не подтверждается.
- **Taker delay** (задержка матчинга marketable-ордеров на крипто-рынках) [docs changelog]:
  - 250 мс;
  - **50 мс** с 2026-08-17 11:00 UTC;
  - **150 мс** с 2026-09-04 14:00 UTC.

  Это «speed bump» в пользу мейкеров. Важно и для гипотезы 3 (опережение спота), и для копирования.
- **Ребейты:** Taker Rebate Program по тирам; у него сейчас Gold [live public-profile].
  Итоги из `/v2/user-stats`: `maker_rebate` $34 239, `taker_rebate` $4 522, `reward_income` $4 541, `fees_paid` −$36 282.
- **Следствие для пар:** taker-покупка пары 0.49 + 0.49 обходится ≈ $1.015, «пара < $1» выгодна только как maker.
- **История режимов:**
  - 2026-01-05 — taker fees на 15m;
  - 2026-02-12 — 5m;
  - 2026-03-06 — вся крипта;
  - 2026-03-30 — Fee Structure V2.

  Вся история @bosona (с 02.06) идёт при текущей формуле.

---

## 6. Контекст для этапа 2 ([live])

- CLOB `prices-history`: шаг ~60 с. Теперь есть и `GET /v2/prices-history` на data-хосте (`bucket_seconds` от 60, `as_of`) [docs].
  После резолва `/book` возвращает 404: **исторического стакана нет**, только live-снимки этапа 3.
- CLOB `/price?side=BUY` возвращает **лучший bid**, `side=SELL` — лучший ask. Проверено против `/book`; это противоречит гайду `agent-skills`.
- Книга: `bids` по возрастанию, `asks` по убыванию (лучшие цены в конце), `timestamp` в мс.
- Binance `data-api.binance.vision`: 1s klines и aggTrades (мс) доступны с мая 2026. Coinbase: 1m-свечи.

## 7. Сверка с наблюдениями в CLAUDE.md

| В CLAUDE.md | Факт [live] |
|---|---|
| «с мая 2026» | профиль создан 2026-05-21, **первая сделка 2026-06-02 21:34 UTC** |
| «~89.6K сделок» | **89 590 — число рынков** (`/v2/user-stats.trades`, v1 `/traded` = 90 258). **Сделок (fills) 540 004–544 939** |
| «объём ~$20.6M» | **20.69M shares**; в USDC **$10.40M** (`volume_usdc`) |
| «PnL ~+$347K» | ✅ `trade_pnl` $348 746; `economic_pnl` $355 766 (с ребейтами и наградами); `realized_market_pnl` $312 524 |
| «~1.7% от оборота» | 1.69% от **shares**; от USDC-оборота **3.35%** |
| почти только крипто Up/Down | ✅ (срез 27.09: 100% крипто Up/Down) |
| BTC, ETH, SOL, XRP, DOGE, BNB | ✅ 27.09: BTC 85%, ETH 7.1%, SOL 4.4%, BNB 1.5%, DOGE 1.4%, XRP 0.9%. HYPE/ZEC не было |
| окна 5m, 15m, 1h, daily | ✅ плюс **4h**. 27.09: 5m 61%, 15m 26%, 1h 8.9%, daily 2.3%, 4h 1.4% |
| держит обе стороны | ✅ 27.09: в 48% рынков покупал и Up, и Down |
| — | **SELL нет вообще** (0 за всю историю): выход через MERGE и REDEEM |
| — | **~91% fills maker** (27.09; по USDC ~80%) и крупные maker rebates, по предварительным данным это маркет-мейкер |

Срезы за 27.09 — одна выборка-сутки. Полная статистика будет на этапах 1 и 4.

## 8. Предложение схемы БД (SQLite, `data/bosona.db`)

Принципы:

- суммы храним целыми в 1e6 и параллельно в `REAL`;
- сырой JSON строки храним;
- у каждой таблицы естественный ключ;
- докачка с перекрытием и `INSERT OR IGNORE`.

```sql
CREATE TABLE markets (
  condition_id        TEXT PRIMARY KEY,
  slug                TEXT NOT NULL UNIQUE,
  event_slug TEXT, series_slug TEXT, question_id TEXT,
  asset               TEXT NOT NULL,             -- btc|eth|sol|xrp|doge|bnb|hype|zec
  timeframe           TEXT NOT NULL,             -- 5m|15m|1h|4h|1d
  window_start_ts     INTEGER NOT NULL,          -- eventStartTime
  window_end_ts       INTEGER NOT NULL,          -- endDate
  accepting_orders_ts INTEGER,                   -- ~за 24 ч до окна
  up_token_id TEXT NOT NULL, down_token_id TEXT NOT NULL,
  resolution_regime   TEXT NOT NULL,             -- chainlink_spot|chainlink_twap30|chainlink_twap60|binance_1h|binance_noon_1m
  resolution_source TEXT, crypto_config_id TEXT, twap_lookback_s INTEGER,
  fee_type TEXT, fee_rate REAL, fee_exponent REAL, fee_taker_only INTEGER, fee_rebate_rate REAL,
  order_min_size REAL, tick_size_last REAL, neg_risk INTEGER,
  raw_json TEXT NOT NULL, fetched_at INTEGER NOT NULL
);
CREATE INDEX ix_markets_asset_tf_start ON markets(asset, timeframe, window_start_ts);

CREATE TABLE resolutions (
  condition_id   TEXT PRIMARY KEY REFERENCES markets(condition_id),
  winner         TEXT,                           -- Up|Down|50-50|NULL
  payout_up REAL, payout_down REAL,
  price_to_beat  REAL, final_price REAL,
  strike_source  TEXT,                           -- gamma_meta|chained|binance_kline|proxy
  closed_ts INTEGER, uma_status TEXT, fetched_at INTEGER NOT NULL
);

-- Fills @bosona (все BUY на сегодня, но SELL поддерживаем)
CREATE TABLE trades (
  trade_uid    TEXT PRIMARY KEY,   -- '{tx}:{token}:{side}:{size_raw}:{price_raw}:{seq}'  (seq — № среди одинаковых строк tx)
  tx_hash      TEXT NOT NULL,
  ts           INTEGER NOT NULL,   -- время блока, сек
  condition_id TEXT NOT NULL,
  token_id     TEXT NOT NULL,
  outcome      TEXT NOT NULL,      -- Up|Down
  outcome_index INTEGER,
  side         TEXT NOT NULL,      -- BUY|SELL
  price        REAL NOT NULL,      -- usdc/size (может быть «нецелым» тиком)
  size_raw     INTEGER NOT NULL,   -- shares*1e6
  usdc_raw     INTEGER NOT NULL,   -- usdc*1e6
  role         TEXT,               -- taker|maker (из разности taker_only=true/false)
  fee_usdc     REAL,               -- taker: C*rate*p*(1-p), maker: 0; при наличии on-chain берём OrderFilled.fee
  log_index INTEGER, block_number INTEGER,                        -- из OrderFilled (опционально)
  order_hash TEXT, order_size REAL, order_limit_price REAL,       -- из calldata matchOrders (опционально); Order.timestamp у него всегда 0
  source       TEXT NOT NULL,      -- data_api_v2|chain|both
  raw_json     TEXT NOT NULL, ingested_at INTEGER NOT NULL
);
CREATE INDEX ix_trades_cond_ts ON trades(condition_id, ts);
CREATE INDEX ix_trades_ts ON trades(ts);
CREATE UNIQUE INDEX ux_trades_chain ON trades(tx_hash, log_index) WHERE log_index IS NOT NULL;

-- Остальная активность: MERGE/REDEEM/SPLIT/REWARD/MAKER_REBATE/TAKER_REBATE/… (тип — свободная строка)
CREATE TABLE activity (
  activity_uid TEXT PRIMARY KEY,   -- '{tx}:{type}:{condition_id}:{token_id}:{seq}'
  tx_hash TEXT NOT NULL, ts INTEGER NOT NULL, type TEXT NOT NULL,
  condition_id TEXT, token_id TEXT, outcome TEXT, outcome_index INTEGER,
  size REAL, usdc_size REAL,
  raw_json TEXT NOT NULL, ingested_at INTEGER NOT NULL
);
CREATE INDEX ix_activity_cond ON activity(condition_id, ts);

-- Контекст на момент сделки (этап 2)
CREATE TABLE market_context (
  trade_uid TEXT PRIMARY KEY REFERENCES trades(trade_uid),
  secs_from_open INTEGER, secs_to_close INTEGER,      -- может быть < 0
  strike REAL, strike_source TEXT,
  spot REAL, spot_source TEXT, dist_bps REAL,
  vol_1m REAL, vol_5m REAL, vol_15m REAL,
  up_mid REAL, down_mid REAL, pair_mid REAL,
  up_bid REAL, up_ask REAL, down_bid REAL, down_ask REAL, book_source TEXT, book_age_ms INTEGER,
  computed_at INTEGER NOT NULL
);

-- Сверка PnL и позиции (из v2)
CREATE TABLE pnl_daily (ts INTEGER PRIMARY KEY, source_block INTEGER, trade_pnl REAL, realized_market_pnl REAL,
  fees_paid REAL, maker_rebate REAL, taker_rebate REAL, reward_income REAL, economic_pnl REAL,
  volume REAL, volume_usdc REAL, trade_count INTEGER, raw_json TEXT);
CREATE TABLE positions (token_id TEXT PRIMARY KEY, condition_id TEXT, status TEXT, total_size REAL, avg_price REAL,
  entry_fees_usdc REAL, realized_pnl REAL, last_event_at INTEGER, raw_json TEXT, fetched_at INTEGER);

-- Кэши внешних рядов и состояние выгрузки
CREATE TABLE spot_bars (source TEXT, symbol TEXT, interval TEXT, open_ts_ms INTEGER,
  o REAL, h REAL, l REAL, c REAL, v REAL, PRIMARY KEY (source, symbol, interval, open_ts_ms));
CREATE TABLE clob_price_history (token_id TEXT, t INTEGER, p REAL, PRIMARY KEY (token_id, t));
CREATE TABLE sync_state (source TEXT, scope TEXT, last_ts INTEGER, updated_at INTEGER, note TEXT,
  PRIMARY KEY (source, scope));

-- Этап 3 (позже): book_snapshots(token_id, ts_ms, recv_ms, bids_json, asks_json, hash),
--                 detections(trade_uid, block_ts, detected_ms, latency_ms, spot_at_detect, mid_at_detect)
```

Контроль полноты после выгрузки:

- `COUNT(trades)` против `/v2/user-volume.trade_count`;
- `SUM(fee_usdc)` против `fees_paid`;
- поденный PnL против `pnl_daily`.

Эпизоды для этапа 4 — это `VIEW` поверх `trades` и `activity` с группировкой по `condition_id`.
