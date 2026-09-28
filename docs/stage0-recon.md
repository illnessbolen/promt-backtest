# Этап 0: разведка источников данных (@bosona)

Дата: 2026-09-28. Кошелёк: `0xc2ad03f79ca3f3c17d8c7de2612ce0c89b7d40ed`.

Как помечена надёжность утверждений:

- **[live]**: проверено живым запросом из этого окружения; сырые ответы лежат в `docs/samples/stage0/`.
- **[src]**: взято из официального кода Polymarket на GitHub: `Polymarket/rs-clob-client` @ `dc8f1e5` (2026-05-11) и `Polymarket/ctf-exchange-v2` @ HEAD. Первоисточник, но живым запросом не проверено.
- **[web]**: взято из выдачи веб-поиска: сниппеты страниц docs.polymarket.com и сторонних статей. Сам docs.polymarket.com из окружения недоступен, поэтому эти утверждения нужно перепроверить.

---

## 0. Сеть: что доступно из окружения

| Хост | Статус | Нужен для |
|---|---|---|
| `gamma-api.polymarket.com` | ✅ 200 | метаданные рынков, правила резолва, strike/final |
| `clob.polymarket.com` (REST) | ✅ 200 | книга, цены, tick/fee, prices-history |
| `data-api.binance.vision` | ✅ 200 | Binance spot klines 1s / aggTrades (зеркало только для рыночных данных) |
| `api.exchange.coinbase.com` | ✅ 200 | Coinbase BTC-USD (лучший прокси Chainlink, см. §4.4) |
| `data-api.polymarket.com` | ❌ **403 от прокси** | **/activity, /trades, /positions, /closed-positions: ключевой источник этапа 1** |
| `docs.polymarket.com` | ❌ 403 (и через WebFetch тоже) | сверка с документацией |
| Polygon RPC (`polygon-rpc.com`, publicnode, drpc, ankr, llamarpc), polygonscan | ❌ 403 | запасной on-chain источник |
| `api.goldsky.com` (subgraph) | ❌ 403 | запасной источник |
| `ws-live-data.polymarket.com`, `ws-subscriptions-clob.polymarket.com` | ❌ 403 | этап 3 (RTDS Chainlink, WS книги) |
| `api.binance.com` | ⚠️ 451 (гео-блок) | заменяется `data-api.binance.vision` |

**Нужно от тебя:** добавить в allowlist окружения (Network access в настройках environment) как минимум
`data-api.polymarket.com` и `docs.polymarket.com`, плюс один Polygon RPC (например `polygon-bor-rpc.publicnode.com`).
Для этапа 3 понадобятся ещё `ws-live-data.polymarket.com`, `ws-subscriptions-clob.polymarket.com` и `data-stream.binance.vision`.
После этого я прогоню проверки из §1.4 и закрою пробелы, помеченные [src] и [web].

---

## 1. Data API: `/activity`, `/trades`, `/positions`, `/closed-positions`

⚠️ Живых ответов нет: хост заблокирован. Ниже схема из официального Rust-клиента Polymarket ([src], файлы `src/data/types/{request,response}.rs`).

### 1.1 Параметры и лимиты

| Эндпоинт | Ключевые параметры | limit | offset |
|---|---|---|---|
| `GET /activity` | `user` (обяз.), `market` (CSV conditionId) \| `eventId`, `type` (CSV), `start`, `end` (unix-сек), `sortBy`=TIMESTAMP\|TOKENS\|CASH, `sortDirection`=ASC\|DESC, `side` | 0–**500** (по умолч. 100) | 0–**10000** |
| `GET /trades` | `user`, `market` \| `eventId`, `side`, **`takerOnly` (по умолчанию `true`!)**, `filterType`+`filterAmount` | 0–**10000** | 0–**10000** |
| `GET /positions` | `user`, `market` \| `eventId`, `sizeThreshold` (по умолч. 1), `redeemable`, `mergeable`, `sortBy`, `title` | 0–500 | 0–10000 |
| `GET /closed-positions` | `user`, `market` \| `eventId`, `title`, `sortBy` (по умолч. REALIZEDPNL) | 0–**50** | 0–100000 |

Типы активности [src]: `TRADE, SPLIT, MERGE, REDEEM, REWARD, CONVERSION, YIELD, MAKER_REBATE`.

Ловушка: у `/trades` по умолчанию `takerOnly=true`, поэтому без `takerOnly=false` пропадут все его maker-исполнения.

### 1.2 Поля ответов [src]

- **Activity**: `proxyWallet, timestamp (i64, сек), conditionId, type, size, usdcSize, transactionHash, price, asset (token id), side, outcomeIndex, outcome, title, slug, eventSlug, icon, name, pseudonym, bio, profileImage…`
- **Trade**: `proxyWallet, side, asset, conditionId, size, price, timestamp (сек), title, slug, eventSlug, outcome, outcomeIndex, name, pseudonym, …, transactionHash`
- **Position**: `asset, conditionId, size, avgPrice, initialValue, currentValue, cashPnl, percentPnl, totalBought, realizedPnl, percentRealizedPnl, curPrice, redeemable, mergeable, outcome, outcomeIndex, oppositeOutcome, oppositeAsset, endDate, negativeRisk, …`
- **ClosedPosition**: `asset, conditionId, avgPrice, totalBought, realizedPnl, curPrice, timestamp, outcome, outcomeIndex, oppositeOutcome, oppositeAsset, endDate, …`

**Чего в Data API нет:** `logIndex`, роли maker/taker, комиссии и времени точнее секунды.
Поэтому для надёжной дедупликации и роли нужен on-chain источник (§2.3).

### 1.3 Data API v2 [web]

Существует `data-api.polymarket.com/v2/...` (`/v2/trades`, `/v2/activity`) с cursor-пагинацией (`pagination.next_cursor`, без offset).
Лента устроена как keyset-проход и стабильна при одновременной записи. Без `start` окно ограничено тремя годами назад, `start=1` даёт полную историю.
Есть упоминания полей role и fee, но не подтверждены. **Если v2 отдаёт роль и комиссию, этап 1 сильно упрощается. Проверить первым делом.**

### 1.4 Что прогнать сразу после открытия хоста

```bash
U=0xc2ad03f79ca3f3c17d8c7de2612ce0c89b7d40ed; D=https://data-api.polymarket.com
curl "$D/activity?user=$U&limit=5"                                  # структура, типы
curl "$D/activity?user=$U&type=MERGE,REDEEM,SPLIT&limit=5"
curl "$D/activity?user=$U&limit=500&offset=10000"                   # потолок offset
curl "$D/activity?user=$U&limit=500&offset=10500"                   # ожидаем 400
curl "$D/activity?user=$U&start=1790000000&end=1790086400&limit=500" # оконная выборка
curl "$D/activity?user=$U&sortDirection=ASC&limit=5"                 # самая ранняя активность
curl "$D/trades?user=$U&takerOnly=false&limit=5"                     # maker+taker
curl "$D/trades?user=$U&limit=5"                                     # сравнить: только taker
curl "$D/positions?user=$U&limit=5"; curl "$D/closed-positions?user=$U&limit=5"
curl "$D/v2/trades?user=$U&limit=5"; curl "$D/v2/activity?user=$U&limit=5"
```

Отдельно проверить: одна строка TRADE соответствует одному fill или агрегату по tx.
Сверка: число строк за день в Data API против числа `OrderFilled` с его адресом.

---

## 2. Пагинация: можно ли выкачать ~90K сделок

### 2.1 Data API

- Потолок offset равен 10 000 [src], [web]: запросы дальше потолка получают 400, «тихого» обрезания нет.
  Один запрос покрывает не больше ~10 500 строк.
- Обход: **резать по времени** (`start`/`end` в `/activity`), у каждого окна свой бюджет offset.
  ~89.6K сделок за ~130 дней дают в среднем ~700 в день. Окно в 1 сутки с адаптивным делением пополам, если окно упирается в 10K.
  Выходит ~200–400 запросов по 500 строк, это минуты работы.
- Альтернатива: v2 cursor (§1.3), без потолка offset.
- Rate limits [web]: Data API ~1000 запросов / 10 с общий, `/trades` 200/10 с, `/positions` 150/10 с.
  Троттлинг Cloudflare (задержка, а не 429). Gamma 4000/10 с (`/events` 500/10 с, `/markets` 300/10 с).

### 2.2 Gamma (проверено [live])

- `/events` и `/markets`: максимум **100 на страницу**, `offset` больше ~2000 даёт `422 "offset too large, use /events/keyset"`.
- `/events/keyset?...&limit=100&after_cursor=<next_cursor>` работает [live]: 3658 открытых up-or-down событий за 37 страниц.
- `/series`: максимум 50 на страницу.

### 2.3 Запасной источник: on-chain `OrderFilled` (CLOB V2)

**Важно:** 2026-04-28 ~11:00 UTC Polymarket перешёл на **CLOB V2**: новые контракты биржи, новый коллатерал pUSD, новый формат события [web].
Профиль @bosona создан 2026-05-21 [live], значит вся его история на V2.

Адреса (Polygon, [src] README ctf-exchange-v2):

| Контракт | Адрес |
|---|---|
| CTFExchangeV2 (Up/Down рынки, `negRisk=false`) | `0xE111180000d2663C0091e4f400237545B87B996B` |
| NegRiskCtfExchangeV2 | `0xe2222d279d744050d28e00520010520000310F59` |
| pUSD (CollateralToken proxy) | `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB` |
| CtfCollateralAdapter (split/merge/redeem за pUSD) | `0xADa100874d00e3331D00F2007a9c336a65009718` |
| ConditionalTokens (ERC-1155) | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` |

Событие V2 [src] `src/exchange/interfaces/ITrading.sol`:

```
OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker,
            uint8 side, uint256 tokenId, uint256 makerAmountFilled, uint256 takerAmountFilled,
            uint256 fee, bytes32 builder, bytes32 metadata)
topic0 = 0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee
OrdersMatched(bytes32 indexed takerOrderHash, address indexed takerOrderMaker, uint8 side, uint256 tokenId, uint256 makerAmountFilled, uint256 takerAmountFilled)
topic0 = 0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c
```

(V1-событие, для справки: `0xd0a08e8c…f6`, у него другая сигнатура.)

**Роль maker/taker определяется однозначно** (по `Trading.sol`):

- для каждого maker-ордера эмитится `OrderFilled(maker = владелец maker-ордера, taker = владелец taker-ордера)`;
- для taker-ордера эмитится `OrderFilled(maker = владелец taker-ордера, taker = адрес биржи)` и `OrdersMatched`.

Отсюда фильтры логов:

- `topic2 == bosona`: все его исполнения. Если `topic3 == 0xE1111800…`, он **taker**, иначе **maker**;
- `topic3 == bosona`: контрагенты-мейкеры его taker-ордеров, то есть по каким уровням книги он прошёл.

Там же есть `fee` (фактическая комиссия fill'а) и точный порядок (`blockNumber`, `logIndex`).

Суммы в 6 знаках. BUY: `makerAmountFilled` = pUSD, `takerAmountFilled` = токены. SELL наоборот.

Бонус: в V2 подписанный `Order` содержит **`timestamp` в миллисекундах** (время создания ордера).
Он лежит в calldata `matchOrders`, и через `eth_getTransactionByHash` можно получить момент решения бота с точностью до мс. Это ценно для анализа реакции на спот (гипотеза 3).

Split/merge/redeem через адаптер своих событий не эмитят [src]. У пользователя они видны как ERC-1155 `TransferBatch` на ConditionalTokens (`0x4a39dc06…f7fb`):

- merge/redeem: пользователь → адаптер;
- split: адаптер → пользователь;

плюс Transfer pUSD. Проще брать их из `/activity`.

Subgraph [web]: старый Goldsky `orderbook-subgraph` после миграции на V2 неполный. Не полагаться на него без проверки.

---

## 3. Метаданные рынков через Gamma ([live])

### 3.1 Вселенная Up/Down (открытые серии на сегодня)

Активы: **BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, ZEC**.
Таймфреймы: **5m, 15m, 4h, 1h, daily**. По открытым сериям на 28.09 у BTC/ETH/SOL/XRP/DOGE/BNB/HYPE есть все пять таймфреймов, у ZEC только 5m/15m/4h.

| Таймфрейм | slug события/рынка | seriesSlug |
|---|---|---|
| 5m / 15m / 4h | `{btc,eth,sol,xrp,doge,bnb,hype,zec}-updown-{5m,15m,4h}-{unix_ts начала окна}` | `btc-up-or-down-15m` … |
| 1h | `{bitcoin,ethereum,solana,xrp,dogecoin,bnb,hype}-up-or-down-{month}-{day}-{year}-{h}{am,pm}-et` | `btc-up-or-down-hourly` … |
| daily | `{…}-up-or-down-on-{month}-{day}-{year}` (в 2025 без года) | `btc-up-or-down-daily` … |

Кроме крипты, в том же теге `up-or-down` есть ежедневные рынки на акции, индексы и FX (`feeType=finance_prices_fees`). Их отфильтровываем.

### 3.2 Как получить рынок

- по slug: `GET /events/slug/{slug}` или `GET /markets/slug/{slug}`. Работает и для закрытых рынков;
- по conditionId пачкой: `GET /markets?closed=true&condition_ids=…&condition_ids=…` (проверено 50 штук за запрос).
  **Ловушка:** без `closed=true` закрытые рынки молча не возвращаются (`[]`);
- по token id: `GET /markets?closed=true&clob_token_ids=…`;
- CLOB: `GET /markets/{conditionId}` возвращает `tokens[{token_id, outcome, price, winner}]`, так что победитель есть и там.

### 3.3 Ключевые поля (пример `btc-updown-15m-1790616600`, файл `gamma_event_btc_15m_resolved.json`)

```jsonc
"conditionId": "0xf6a64d2d…db41e",
"outcomes": "[\"Up\", \"Down\"]",                 // JSON в строке
"clobTokenIds": "[\"72701660…15502\", \"72842718…78432\"]",  // тот же порядок: [Up, Down]
"eventStartTime": "2026-09-28T17:30:00Z",         // начало окна
"endDate": "2026-09-28T17:45:00Z",                // конец окна
"acceptingOrdersTimestamp": "2026-09-27T17:37:50Z", // торги открыты за ~24 ч ДО окна!
"closedTime": "2026-09-28 17:46:27+00", "umaResolutionStatus": "resolved", "automaticallyResolved": true,
"outcomePrices": "[\"1\", \"0\"]",                 // итог: Up
"orderMinSize": 5, "orderPriceMinTickSize": 0.001, // tick динамический: 0.01 ↔ 0.001 у краёв
"feesEnabled": true, "feeType": "crypto_fees_v2",
"feeSchedule": {"exponent": 1, "rate": 0.07, "takerOnly": true, "rebateRate": 0.2},
"makerBaseFee": 1000, "takerBaseFee": 1000, "makerRebatesFeeShareBps": 10000,
"cryptoMarketConfig": {"id": "btc-15m-twap-60", "asset": "btc", "duration": "15m", "twapEnabled": true, "twapLookbackSeconds": 60},
// на уровне event:
"eventMetadata": {"priceToBeat": 83884.316…, "finalPrice": …}   // strike и итоговая цена
```

---

## 4. Правила разрешения ([live]: описания рынков + сверка чисел)

### 4.1 Сводка

| Рынки | Период | Источник | Правило | Ничья |
|---|---|---|---|---|
| 5m, 15m, 4h | до **2026-08-07 00:00 UTC** | Chainlink Data Stream `{asset}-usd` (спот) | цена в конце окна ≥ цены в начале, тогда Up | Up |
| 15m, 4h | с 2026-08-07 00:00 UTC | Chainlink `{asset}-usd-twap-60s-streams` | TWAP (lookback 60 с) ≥ цены начала, тогда Up | Up |
| 5m | 2026-08-07 → 2026-08-14 00:00 UTC | Chainlink `…-twap-30s-streams` | то же, TWAP 30 с | Up |
| 5m | с 2026-08-14 00:00 UTC | Chainlink `…-twap-60s-streams` | TWAP 60 с | Up |
| 1h | весь период | **Binance `{ASSET}USDT` свеча 1h** | close ≥ open, тогда Up | Up |
| daily | весь период | **Binance `{ASSET}USDT` 1m свеча 12:00 ET** | close(12:00 ET сегодня) > close(12:00 ET вчера), тогда Up | **50/50** |

Дату перехода нашёл бинарным поиском по `cryptoMarketConfigId`/`resolutionSource` для BTC/ETH/SOL/XRP/DOGE/BNB. Даты у всех активов одинаковые.
Это совпадает с анонсом @PolymarketDevs [web].
`cryptoMarketConfigId`: до перехода `btc-15m` (или `null` у рынков до ~августа), после `btc-15m-twap-60`, `btc-5m-twap-30`, `btc-5m-twap-60`.

→ **История @bosona делится на два режима резолва.** Бэктест и анализ обязаны учитывать режим. Особенно это касается поздних входов у самой границы окна: на TWAP-рынке «последний тик» уже не решает.

### 4.2 «Цена открытия» (strike) и итог

- `eventMetadata.priceToBeat` и `eventMetadata.finalPrice` у события Gamma. Проверено:
  - **1h:** `priceToBeat` = open 1h-свечи Binance, `finalPrice` = close. Для рынка 12PM ET 28.09: 83370 / 83723.1, совпадает с `klines 1h` до цента;
  - **daily:** `priceToBeat` = close 1m-свечи 12:00 ET вчера (84487.11), `finalPrice` = close 12:00 ET сегодня (83375.57), совпадает;
  - **5m/15m/4h:** `priceToBeat(N) == finalPrice(N−1)` ровно (цепочка). Strike окна равен итоговой цене (TWAP) предыдущего окна.
- Покрытие метаданных: 96–100% окон в день (замерено по трём дням).
  Пропуски восстанавливаются цепочкой и через `outcomePrices`.
- **`priceToBeat` появляется в Gamma только после резолва предыдущего окна**, то есть на ~1 мин позже старта окна.
  Для live-трекера strike придётся считать самим (RTDS `crypto_prices_chainlink`, [src]: `wss://ws-live-data.polymarket.com`, символы `btc/usd`, ts в мс).
- Задержка резолва (closedTime − конец окна): 5m и 15m ~20–90 с, 1h и daily ~12–13 мин (выборка из ~10 рынков).

### 4.3 Доступность Chainlink

Chainlink Data Streams публично не отдаются: нужны ключи [web]. Исторический тиковый Chainlink недоступен.
Остаются Gamma `priceToBeat`/`finalPrice` (точки на границах окон) и прокси для цены внутри окна.

### 4.4 Качество прокси (замер [live], BTC 15m, 12 окон)

| Прокси | Смещение от Chainlink strike | σ |
|---|---|---|
| Coinbase `BTC-USD`, close 1m-свечи перед границей (до TWAP) | **−0.07 bps** | 0.63 bps |
| Binance `BTCUSDT` 1s close перед границей (до TWAP, 05.08) | −8.54 bps | 0.50 bps |
| Binance 1s, среднее за 60 с (TWAP-режим, 28.09): BTC / ETH / SOL | −3.26 / −3.37 / −3.51 bps | ~0.6 bps |

Вывод: Binance 1s подходит с **скользящей поправкой на базис USDT/USD**, а базис дрейфует (−8.5 bps в августе, −3.3 bps в сентябре).
Калибруем её по `priceToBeat`. Coinbase USD почти без смещения, но только 1m-свечи (тики через `/trades`).

---

## 5. Комиссии

Текущее состояние, [live] с Gamma/CLOB для всех крипто Up/Down (5m…daily):

- `feeType: crypto_fees_v2`, `feeSchedule {rate: 0.07, exponent: 1, takerOnly: true, rebateRate: 0.2}`;
- CLOB `/fee-rate` возвращает `{"base_fee": 1000}`, `maker_base_fee = taker_base_fee = 1000`. Это потолок для подписи ордера, а не фактическая ставка.

Формула [web] (docs «Fees», сниппеты): **`fee = C · rate · p · (1 − p)`** в USDC, C — число shares. Платит только taker, maker платит 0.

| Цена входа p | Комиссия taker, ¢ за share | % от номинала (= rate·(1−p)) |
|---|---|---|
| 0.50 | 1.75 | 3.50% |
| 0.70 | 1.47 | 2.10% |
| 0.90 | 0.63 | 0.70% |
| 0.95 | 0.33 | 0.35% |
| 0.99 | 0.07 | 0.07% |

- **Maker rebates:** 20% собранных taker-комиссий раздаётся мейкерам ежедневно [web].
- **Taker Rebate Program** [web]: тиры по 30-дневному weighted volume. Профиль @bosona сейчас: `takerTier: 3 "Gold"`, `weightedVolume: 493229` [live]. Gold соответствует ~18% возврата taker-комиссий [web].
- **История** [web]: taker fees на 15m-крипте с января 2026, на всей крипте (1h/4h/daily) с 2026-03-06, смена формулы около 2026-03-30.
  Crypto rate снижен **0.072 → 0.07 в июле 2026**.
  Gamma показывает *текущий* `feeSchedule` даже у апрельских рынков, так что историю ставок по Gamma не восстановить.
  **Фактическую комиссию каждого fill'а надо брать из `OrderFilled.fee` on-chain.**
  Контракт лишь проверяет, что `fee ≤ maxFeeRateBps` (по умолчанию 5%) от cash-объёма [src].
- Следствие для гипотезы 1 (пара < $1): taker-покупка пары по 0.49+0.49 даёт комиссию ≈ 3.5¢, итого пара ≈ $1.015.
  **Парная «арбитражка» в роли taker невыгодна**, пока сумма пары не ниже ~0.965. Поэтому роль maker или taker для него ключевая.

---

## 6. Контекст для этапа 2 ([live])

- CLOB `prices-history`: даже с `fidelity=1` шаг **~60 с** (≈5 точек на 5m-окно).
  После резолва `/book` возвращает 404, **исторического стакана нет**. Стакан на момент сделки получим только live-снимками (этап 3).
- CLOB `/price?side=BUY` возвращает **лучший bid**, `side=SELL` возвращает **лучший ask** (проверено против `/book`, 3 раза).
  Это противоречит комментарию в `Polymarket/agent-skills`, поэтому лучше опираться на `/book`.
- Книга: `bids` по возрастанию, `asks` по убыванию (лучшие цены в конце), `timestamp` в мс, `hash`.
- Binance `data-api.binance.vision`: `klines interval=1s` и `aggTrades` (мс) доступны за май 2026 и раньше. Вес видно в `x-mbx-used-weight-1m`.
- Coinbase: 1m-свечи `BTC-USD` и т. д.

## 7. Расхождения с наблюдениями в CLAUDE.md и сомнения

1. **Главное: не проверены** ~89.6K сделок, $20.6M, +$347K, доля парных позиций и пр. Data API закрыт.
   Из доступного: профиль создан 2026-05-21 (сходится с «с мая»), `name: bosona`, taker-тир Gold.
2. Список рынков шире, чем в CLAUDE.md: есть ещё **4h**, а также **HYPE и ZEC**. Что из этого торгует он, станет видно по данным.
3. Правила 5m/15m/4h **изменились в середине его истории** (07.08 и 14.08). Смешивать периоды в одной статистике нельзя.
4. Торги по окну открыты за ~24 ч до его начала, так что «секунды от открытия окна» бывают отрицательными.
5. Комиссии существенны (до 3.5% номинала у taker около 50¢) и менялись во времени. PnL и EV считать по фактическим `fee` из on-chain.
6. Tick size динамический: 0.01 в середине и 0.001 у краёв. Это важно для моделирования исполнения по 0.99.
7. У части старых событий в Gamma висит `closed=false` (например, `btc-updown-5m-1766162100` декабря 2025). При выборках фильтровать по `endDate`, а не только по `closed`.

---

## 8. Предложение схемы БД (SQLite, `data/bosona.db`)

Принципы:

- суммы храним в целых базовых единицах (1e6) плюс `REAL` для удобства;
- «сырые» ответы храним как JSON, чтобы можно было перепарсить;
- каждая таблица имеет естественный ключ дедупликации;
- `sync_state` делает выгрузку идемпотентной.

```sql
-- Рынки (одна строка на conditionId)
CREATE TABLE markets (
  condition_id        TEXT PRIMARY KEY,          -- 0x…
  slug                TEXT NOT NULL UNIQUE,
  event_slug          TEXT, series_slug TEXT,
  asset               TEXT NOT NULL,             -- btc|eth|sol|xrp|doge|bnb|hype|zec
  timeframe           TEXT NOT NULL,             -- 5m|15m|1h|4h|1d
  window_start_ts     INTEGER NOT NULL,          -- eventStartTime, unix сек
  window_end_ts       INTEGER NOT NULL,          -- endDate
  accepting_orders_ts INTEGER,                   -- ~за 24 ч до окна
  up_token_id         TEXT NOT NULL, down_token_id TEXT NOT NULL,
  resolution_regime   TEXT NOT NULL,             -- chainlink_spot|chainlink_twap30|chainlink_twap60|binance_1h|binance_noon_1m
  resolution_source   TEXT, crypto_config_id TEXT, twap_lookback_s INTEGER,
  fee_type TEXT, fee_rate REAL, fee_exponent REAL, fee_taker_only INTEGER, fee_rebate_rate REAL,
  order_min_size REAL, tick_size_last REAL, neg_risk INTEGER,
  raw_json            TEXT NOT NULL, fetched_at INTEGER NOT NULL
);
CREATE INDEX ix_markets_asset_tf_start ON markets(asset, timeframe, window_start_ts);

-- Итоги рынков
CREATE TABLE resolutions (
  condition_id   TEXT PRIMARY KEY REFERENCES markets(condition_id),
  winner         TEXT,                           -- Up|Down|50-50|NULL (не резолвлен)
  payout_up REAL, payout_down REAL,              -- из outcomePrices
  price_to_beat  REAL, final_price REAL,         -- strike / итог
  strike_source  TEXT,                           -- gamma_meta|chained|binance_kline|proxy
  closed_ts      INTEGER, uma_status TEXT, fetched_at INTEGER NOT NULL
);

-- Сделки (fills) @bosona: канонический слой
CREATE TABLE trades (
  trade_uid     TEXT PRIMARY KEY,   -- '{tx}:{logIndex}' если on-chain; иначе 'api:{tx}:{asset}:{side}:{size_raw}:{price}:{seq}'
  tx_hash       TEXT NOT NULL,
  log_index     INTEGER,            -- из OrderFilled (NULL, пока только Data API)
  block_number  INTEGER,
  ts            INTEGER NOT NULL,   -- unix сек (время блока)
  order_ts_ms   INTEGER,            -- Order.timestamp из calldata (мс), если достанем
  condition_id  TEXT NOT NULL REFERENCES markets(condition_id),
  asset         TEXT NOT NULL,      -- token id
  outcome       TEXT NOT NULL,      -- Up|Down
  side          TEXT NOT NULL,      -- BUY|SELL
  price         REAL NOT NULL,
  size_raw      INTEGER NOT NULL,   -- shares * 1e6
  usdc_raw      INTEGER NOT NULL,   -- pUSD * 1e6
  fee_raw       INTEGER,            -- OrderFilled.fee
  role          TEXT,               -- maker|taker|NULL
  order_hash    TEXT, counterparty TEXT,
  source        TEXT NOT NULL,      -- data_api|chain|both
  raw_json      TEXT, ingested_at INTEGER NOT NULL,
  UNIQUE (tx_hash, log_index)
);
CREATE INDEX ix_trades_cond_ts ON trades(condition_id, ts);
CREATE INDEX ix_trades_ts ON trades(ts);

-- Не-торговая активность: SPLIT/MERGE/REDEEM/REWARD/CONVERSION/YIELD/MAKER_REBATE
CREATE TABLE activity (
  activity_uid  TEXT PRIMARY KEY,   -- '{tx}:{type}:{condition_id}:{asset}:{seq}'
  tx_hash TEXT NOT NULL, ts INTEGER NOT NULL, type TEXT NOT NULL,
  condition_id TEXT, asset TEXT, outcome_index INTEGER,
  size REAL, usdc_size REAL, price REAL,
  raw_json TEXT NOT NULL, ingested_at INTEGER NOT NULL
);
CREATE INDEX ix_activity_cond ON activity(condition_id, ts);

-- Контекст на момент сделки (этап 2)
CREATE TABLE market_context (
  trade_uid TEXT PRIMARY KEY REFERENCES trades(trade_uid),
  secs_from_open INTEGER, secs_to_close INTEGER,      -- может быть <0 (вход до окна)
  strike REAL, strike_source TEXT,
  spot REAL, spot_source TEXT,                        -- coinbase_1m|binance_1s_adj|chainlink_rtds
  dist_bps REAL,                                      -- (spot/strike-1)*1e4
  vol_1m REAL, vol_5m REAL, vol_15m REAL,             -- реализованная вола спота до сделки
  up_mid REAL, down_mid REAL, pair_mid REAL,          -- из prices-history (~1 мин) или снимков
  up_bid REAL, up_ask REAL, down_bid REAL, down_ask REAL, book_source TEXT, book_age_ms INTEGER,
  computed_at INTEGER NOT NULL
);

-- Кэши внешних рядов
CREATE TABLE spot_bars (
  source TEXT, symbol TEXT, interval TEXT, open_ts_ms INTEGER,
  o REAL, h REAL, l REAL, c REAL, v REAL,
  PRIMARY KEY (source, symbol, interval, open_ts_ms)
);
CREATE TABLE clob_price_history (token_id TEXT, t INTEGER, p REAL, PRIMARY KEY (token_id, t));

-- Состояние выгрузки (идемпотентность / докачка)
CREATE TABLE sync_state (
  source TEXT, scope TEXT,           -- напр. ('data_api.activity','0xc2ad…'), ('chain.orderfilled','maker')
  cursor TEXT, last_ts INTEGER, last_block INTEGER, updated_at INTEGER,
  PRIMARY KEY (source, scope)
);

-- Сверка PnL с Data API (снимки)
CREATE TABLE positions_snapshot (snap_ts INTEGER, asset TEXT, condition_id TEXT, raw_json TEXT, PRIMARY KEY (snap_ts, asset));
CREATE TABLE closed_positions   (asset TEXT PRIMARY KEY, condition_id TEXT, realized_pnl REAL, avg_price REAL, total_bought REAL, ts INTEGER, raw_json TEXT);

-- Этап 3 (позже): book_snapshots(token_id, ts_ms, recv_ms, bids_json, asks_json, hash),
--                 detections(trade_uid, block_ts, detected_ms, latency_ms, spot_at_detect, mid_at_detect)
```

Дедупликация:

- канонический ключ fill'а — `(tx_hash, log_index)` из `OrderFilled`;
- пока on-chain недоступен, строки Data API получают синтетический `trade_uid` с `seq`, порядковым номером среди полностью одинаковых строк одного tx;
- при появлении on-chain строки сливаем по `(tx_hash, asset, side, size_raw)` и проставляем `log_index`, `role`, `fee_raw`, `source='both'`.

Эпизоды для этапа 4 — это `VIEW` поверх `trades` с группировкой по `condition_id`.
