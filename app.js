/* ============================================================
 * Gate 股票合约 × 真实市场 开平仓价差监控
 *
 * 双数据源模式：
 *   本地模式 : 同源 /api/health 可用 → REST 轮询（完整映射清单）
 *   WS 模式   : 云端静态托管 → 直连 Gate 期货 WebSocket
 *
 * 真实市场行情（腾讯行情接口，CORS 可用，浏览器直连）：
 *   A股/港股 : 实时买一/卖一盘口（收盘后为最新价）
 *   美股     : 最新价（腾讯不提供美股盘口，标注近似）
 *   汇率     : open.er-api.com（USD 基准，日更缓存）
 *
 * 价差指标（统一"Gate − 市场"视角，Gate 侧为市价单成交价）：
 *   开仓差价% = (Gate买一 − 市场卖一×汇率) / (市场卖一×汇率)   [市场市价买 + Gate 市价卖开仓]
 *   清仓差价% = (Gate卖一 − 市场买一×汇率) / (市场买一×汇率)   [市场市价卖 + Gate 市价买平仓]
 * ============================================================ */

'use strict';

const $ = (id) => document.getElementById(id);
const fmt = (n, d = 2) => (n === null || n === undefined || isNaN(n)) ? '—' : Number(n).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
const fmtVol = (v) => {
  if (v === null || v === undefined || isNaN(v)) return '—';
  if (v >= 1e8) return (v / 1e8).toFixed(2) + '亿';
  if (v >= 1e4) return (v / 1e4).toFixed(1) + '万';
  return fmt(v, 0);
};

const WS_URL = 'wss://fx-ws.gateio.ws/v4/ws/usdt/';
const WS_SUB_BATCH = 100;
const TENCENT_URL = 'https://qt.gtimg.cn/q=';
const TENCENT_BATCH = 60;
const FX_URL = 'https://open.er-api.com/v6/latest/USD';
const HISTORY_BUCKET_MS = 5 * 60 * 1000;
const HISTORY_RETENTION_MS = 3 * 24 * 60 * 60 * 1000;
const HISTORY_DB_NAME = 'gate-spread-radar-history';
const HISTORY_STORE = 'samples';

let STOCK_NAMES = {};   // { CODE_USDT: {name, market, aCode, tencent, mkt, cur} }
let lastSnapshot = null;

const state = {
  mode: 'local',          // local | ws
  view: 'ashare',
  search: '',
  sortKey: 'openArbPct',
  sortDir: -1,
  threshold: 5.0,
  interval: 10,
  autoRefresh: true,
  theme: localStorage.getItem('gssm-theme') || 'dark',
  notified: new Set(),
  timer: null,
  lastUpdate: null,
  loading: false,
};

/* ---------------- 市场行情状态 ---------------- */
const marketState = {
  quotes: new Map(),   // tencentCode -> {bid, ask, last, chg, time, approx}
  fx: { CNY: null, HKD: null, fetchedAt: 0 },
  fxTimer: null,
  loading: false,
  queued: false,
};

/* ---------------- 本机历史价差状态 ---------------- */
const historyState = {
  dbPromise: null,
  memory: new Map(),
  savedBuckets: {},
  pendingBuckets: new Set(),
  recordTimers: {},
  activeContract: null,
  rangeHours: 4,
  chartPoints: [],
  chartWidth: 960,
  chartSince: 0,
  chartNow: 0,
  renderToken: 0,
  previousFocus: null,
  previousContract: null,
};

/* ---------------- 模式探测 ---------------- */

async function detectMode() {
  try {
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), 3500);
    const res = await fetch('/api/health', { signal: ctrl.signal });
    clearTimeout(to);
    if (res.ok) {
      const j = await res.json();
      if (j.ok) return 'local';
    }
  } catch (e) { /* 静态托管无 /api → 走 WS */ }
  return 'ws';
}

/* ================= 本地模式（REST 轮询） ================= */

async function fetchTickersLocal() {
  if (state.loading) return;
  state.loading = true;
  setStatus('刷新中…', false);
  try {
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), 15000);
    const res = await fetch('/api/tickers', { signal: ctrl.signal });
    clearTimeout(to);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const json = await res.json();
    if (!json.ok) throw new Error(json.error || 'unknown');
    state.lastUpdate = json.ts;
    buildRowsLocal(json.data);
    setStatus('正常', false);
    $('statTime').textContent = 'Gate ' + new Date(json.ts).toLocaleTimeString('zh-CN', { hour12: false });
  } catch (e) {
    setStatus('连接失败', true);
    $('statTime').textContent = e.message || String(e);
    console.error(e);
  } finally {
    state.loading = false;
  }
}

function buildRowsLocal(tickers) {
  const stocks = tickers.filter((t) => STOCK_NAMES[t.contract]);
  const prev = new Map((lastSnapshot || []).map((r) => [r.contract, r]));
  const isFirst = !lastSnapshot;
  lastSnapshot = stocks.map((t) => tickerToRow(t.contract, {
    bid: parseFloat(t.highest_bid), ask: parseFloat(t.lowest_ask),
    last: parseFloat(t.last) || 0, chg: parseFloat(t.change_percentage) || 0,
    vol: parseFloat(t.volume_24h_settle) || 0, oi: parseFloat(t.total_size) || 0,
  }, !isFirst && !prev.has(t.contract)));
  render();
}

/* ================= WS 模式（实时推送） ================= */

const wsState = { ws: null, map: new Map(), reconnectDelay: 1000, heartbeat: null, renderTimer: null, connected: false, subscribed: [] };

function connectWS() {
  try { wsState.ws && wsState.ws.close(); } catch (e) {}
  setStatus('WS 连接中…', false);
  let ws;
  try {
    ws = new WebSocket(WS_URL);
  } catch (e) {
    setStatus('WS 不可用', true);
    scheduleReconnect();
    return;
  }
  wsState.ws = ws;

  ws.onopen = () => {
    wsState.connected = true;
    wsState.reconnectDelay = 1000;
    setStatus('WS 实时推送', false);
    subscribeView();
    clearInterval(wsState.heartbeat);
    wsState.heartbeat = setInterval(() => {
      try { ws.send(JSON.stringify({ time: Math.floor(Date.now() / 1000), channel: 'futures.ping' })); } catch (e) {}
    }, 30000);
  };

  ws.onmessage = (e) => {
    let m;
    try { m = JSON.parse(e.data); } catch (err) { return; }
    if (m.event !== 'update' || !m.result) return;
    if (m.channel === 'futures.tickers') {
      const r = Array.isArray(m.result) ? m.result[0] : m.result;
      if (r && STOCK_NAMES[r.contract]) {
        const d = wsState.map.get(r.contract) || {};
        d.last = parseFloat(r.last) || d.last || 0;
        d.chg = parseFloat(r.change_percentage) || 0;
        d.vol = parseFloat(r.volume_24h_settle) || 0;
        d.oi = parseFloat(r.total_size) || 0;
        wsState.map.set(r.contract, d);
      }
    } else if (m.channel === 'futures.book_ticker') {
      const r = m.result;
      if (r && r.s && STOCK_NAMES[r.s]) {
        const d = wsState.map.get(r.s) || {};
        d.bid = parseFloat(r.b);
        d.ask = parseFloat(r.a);
        wsState.map.set(r.s, d);
      }
    }
    scheduleWSRender();
  };

  ws.onerror = () => { try { ws.close(); } catch (e) {} };

  ws.onclose = () => {
    wsState.connected = false;
    clearInterval(wsState.heartbeat);
    if (document.visibilityState !== 'hidden') setStatus('连接断开，重连中', true);
    scheduleReconnect();
  };
}

function scheduleReconnect() {
  clearTimeout(wsState.reconnectTimer);
  const delay = wsState.reconnectDelay;
  wsState.reconnectDelay = Math.min(delay * 2, 30000);
  wsState.reconnectTimer = setTimeout(connectWS, delay);
}

function currentViewContracts() {
  const all = Object.keys(STOCK_NAMES);
  if (state.view === 'ashare') return all.filter((c) => STOCK_NAMES[c].market === 'A股');
  if (state.view === 'hk') return all.filter((c) => STOCK_NAMES[c].mkt === '港股');
  return all;
}

function currentViewTencentCodes() {
  return currentViewContracts()
    .map((c) => STOCK_NAMES[c].tencent)
    .filter((t) => t);
}

function subscribeView() {
  const ws = wsState.ws;
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const contracts = currentViewContracts();
  if (wsState.subscribed.length) {
    sendSub(ws, 'futures.tickers', 'unsubscribe', wsState.subscribed);
    sendSub(ws, 'futures.book_ticker', 'unsubscribe', wsState.subscribed);
  }
  for (let i = 0; i < contracts.length; i += WS_SUB_BATCH) {
    const batch = contracts.slice(i, i + WS_SUB_BATCH);
    sendSub(ws, 'futures.tickers', 'subscribe', batch);
    sendSub(ws, 'futures.book_ticker', 'subscribe', batch);
  }
  wsState.subscribed = contracts;
}

function sendSub(ws, channel, event, payload) {
  try {
    ws.send(JSON.stringify({ time: Math.floor(Date.now() / 1000), channel, event, payload }));
  } catch (e) {}
}

function scheduleWSRender() {
  state.lastUpdate = Date.now();
  if (wsState.renderTimer) return;
  wsState.renderTimer = setTimeout(() => {
    wsState.renderTimer = null;
    buildRowsWS();
  }, 400);
}

function buildRowsWS() {
  const isFirst = !lastSnapshot;
  const prev = new Map((lastSnapshot || []).map((r) => [r.contract, r]));
  const rows = [];
  wsState.map.forEach((d, contract) => {
    if (!STOCK_NAMES[contract]) return;
    rows.push(tickerToRow(contract, d, !isFirst && !prev.has(contract)));
  });
  Object.keys(STOCK_NAMES).forEach((c) => {
    if (!wsState.map.has(c)) rows.push(tickerToRow(c, {}, !isFirst && !prev.has(c)));
  });
  lastSnapshot = rows;
  render();
  $('statTime').textContent = 'WS 实时 · ' + new Date().toLocaleTimeString('zh-CN', { hour12: false });
}

/* ================= 真实市场行情（腾讯 + 汇率） ================= */

async function loadFx() {
  const cached = localStorage.getItem('gssm-fx');
  if (cached) {
    try {
      const c = JSON.parse(cached);
      if (Date.now() - c.fetchedAt < 6 * 3600 * 1000) {
        marketState.fx = c;
        return;
      }
    } catch (e) {}
  }
  try {
    const res = await fetch(FX_URL);
    const j = await res.json();
    if (j.result === 'success' && j.rates) {
      marketState.fx = { CNY: j.rates.CNY, HKD: j.rates.HKD, fetchedAt: Date.now() };
      localStorage.setItem('gssm-fx', JSON.stringify(marketState.fx));
    }
  } catch (e) {
    console.error('汇率获取失败', e);
  }
}

function fxRate(cur) {
  if (cur === 'USD') return 1;
  return marketState.fx[cur] ? 1 / marketState.fx[cur] : null; // 本地货币 → USDT
}

async function fetchMarketQuotes() {
  if (marketState.loading) {
    marketState.queued = true;
    return;
  }
  const codes = [...new Set(currentViewTencentCodes())];
  if (!codes.length) return;
  marketState.loading = true;
  try {
    const batches = [];
    for (let i = 0; i < codes.length; i += TENCENT_BATCH) {
      batches.push(codes.slice(i, i + TENCENT_BATCH));
    }
    const results = await Promise.allSettled(batches.map(async (batch) => {
      const ctrl = new AbortController();
      const timeout = setTimeout(() => ctrl.abort(), 6000);
      try {
        const res = await fetch(TENCENT_URL + batch.join(','), { signal: ctrl.signal });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const buf = await res.arrayBuffer();
        return new TextDecoder('gbk').decode(buf);
      } finally {
        clearTimeout(timeout);
      }
    }));
    for (const result of results) {
      if (result.status !== 'fulfilled') {
        console.warn('市场行情批次获取失败', result.reason);
        continue;
      }
      const text = result.value;
      for (const line of text.split(';')) {
        const m = line.trim().match(/^v_([\w.]+)="(.*)"$/);
        if (!m) continue;
        const parts = m[2].split('~');
        if (parts.length < 33 || !parts[0]) continue;
        const last = parseFloat(parts[3]);
        const rawBid = parseFloat(parts[9]);
        const rawAsk = parseFloat(parts[19]);
        if (!isFinite(last) || last <= 0) continue;
        marketState.quotes.set(m[1], {
          last,
          // 收盘或盘口为空时，用最新价作为近似值，避免整只股票显示无行情。
          bid: rawBid > 0 ? rawBid : last,
          ask: rawAsk > 0 ? rawAsk : last,
          chg: parseFloat(parts[32]),
          time: parts[30],
          approx: rawBid <= 0 || rawAsk <= 0,
        });
      }
    }
    render();
  } catch (e) {
    console.error('市场行情获取失败', e);
  } finally {
    marketState.loading = false;
    if (marketState.queued) {
      marketState.queued = false;
      fetchMarketQuotes();
    }
  }
}

function marketBidUsd(tencent) {
  const q = marketState.quotes.get(tencent);
  if (!q) return null;
  const info = Object.values(STOCK_NAMES).find((v) => v.tencent === tencent);
  const fx = fxRate(info ? info.cur : 'USD');
  if (!fx || !q.bid || !q.ask) return null;
  return { bid: q.bid * fx, ask: q.ask * fx, approx: q.bid === q.ask, time: q.time };
}

/* ================= 通用行构建 ================= */

function tickerToRow(contract, t, fresh) {
  const info = STOCK_NAMES[contract];
  // HKD 计价变体合约（*HKD_USDT）：Gate 侧价格单位是港元，统一换算成 USDT
  const unitFx = info.unit === 'HKD'
    ? (marketState.fx.HKD ? 1 / marketState.fx.HKD : 1)
    : 1;
  const rawBid = isFinite(t.bid) && t.bid > 0 ? t.bid : 0;
  const rawAsk = isFinite(t.ask) && t.ask > 0 ? t.ask : 0;
  const bid = rawBid * unitFx;
  const ask = rawAsk * unitFx;
  const mid = (bid + ask) / 2;
  const hasDepth = bid > 0 && ask > 0 && ask >= bid;
  const spread = hasDepth ? ask - bid : null;
  const spreadPct = hasDepth && mid > 0 ? (spread / mid) * 100 : null;

  const info2 = STOCK_NAMES[contract];
  let mkt = null;
  if (info2 && info2.tencent) {
    const q = marketState.quotes.get(info2.tencent);
    if (q && q.bid > 0 && q.ask > 0) {
      const fx = fxRate(info2.cur || 'USD');
      if (fx) {
        const mBid = q.bid * fx, mAsk = q.ask * fx;
        mkt = {
          bid: mBid, ask: mAsk,
          approx: info2.mkt === '美股' || q.approx || q.bid === q.ask,
          time: q.time,
          last: q.last, cur: info2.cur || 'USD',
        };
      }
    }
  }

  let openArbPct = null, openArb = null, closeArbPct = null, closeArb = null;
  let prem = null; // Gate 中间价相对市场中间价的溢/折价（+ 溢价）
  if (hasDepth && mkt) {
    openArb = bid - mkt.ask;                    // 开仓：Gate 市价卖（吃买一）− 市场市价买（吃卖一）
    openArbPct = mkt.ask > 0 ? (openArb / mkt.ask) * 100 : null;
    closeArb = ask - mkt.bid;                   // 清仓：Gate 市价买（吃卖一）− 市场市价卖（吃买一）
    closeArbPct = mkt.bid > 0 ? (closeArb / mkt.bid) * 100 : null;
    const mMid = (mkt.bid + mkt.ask) / 2;
    const gMid = (bid + ask) / 2;
    prem = mMid > 0 ? (gMid - mMid) / mMid * 100 : null;
  }

  return {
    contract, name: info.name, market: info.market, aCode: info.aCode || '',
    hkCode: info.tencent && info.tencent.startsWith('hk') ? info.tencent.slice(2) + '.HK' : '',
    mktMkt: info.mkt || null, mktApprox: mkt ? mkt.approx : false,
    mktTime: mkt ? mkt.time : null, mktCur: mkt ? mkt.cur : null,
    bid, ask, spread, spreadPct,
    mktBid: mkt ? mkt.bid : null, mktAsk: mkt ? mkt.ask : null,
    openArb, openArbPct, closeArb, closeArbPct, prem,
    last: t.last || 0, chg: t.chg || 0, vol: t.vol || 0, oi: t.oi || 0,
    fresh: !!fresh,
  };
}

/* ================= 历史价差（IndexedDB，本机滚动 3 天） ================= */

function openHistoryDb() {
  if (historyState.dbPromise) return historyState.dbPromise;
  if (!('indexedDB' in window)) {
    historyState.dbPromise = Promise.resolve(null);
    return historyState.dbPromise;
  }
  historyState.dbPromise = new Promise((resolve) => {
    const request = indexedDB.open(HISTORY_DB_NAME, 1);
    request.onupgradeneeded = () => {
      const db = request.result;
      const store = db.createObjectStore(HISTORY_STORE, { keyPath: 'id' });
      store.createIndex('contractTs', ['contract', 'ts']);
      store.createIndex('ts', 'ts');
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => {
      console.warn('历史价差数据库不可用，已退回当前页面内存', request.error);
      resolve(null);
    };
    request.onblocked = () => resolve(null);
  });
  return historyState.dbPromise;
}

function rowsForView(view) {
  const rows = lastSnapshot || [];
  if (view === 'ashare') return rows.filter((r) => r.market === 'A股');
  if (view === 'hk') return rows.filter((r) => r.mktMkt === '港股');
  return rows;
}

function compactHistoryNumber(value) {
  return value === null || !isFinite(value) ? null : Number(value.toFixed(4));
}

function historyRecord(row, bucket) {
  return {
    id: row.contract + ':' + bucket,
    contract: row.contract,
    ts: bucket,
    openArbPct: compactHistoryNumber(row.openArbPct),
    closeArbPct: compactHistoryNumber(row.closeArbPct),
  };
}

function saveHistoryToMemory(records) {
  const cutoff = Date.now() - HISTORY_RETENTION_MS;
  records.forEach((record) => {
    const list = historyState.memory.get(record.contract) || [];
    const kept = list.filter((item) => item.ts >= cutoff && item.id !== record.id);
    kept.push(record);
    historyState.memory.set(record.contract, kept);
  });
}

async function saveHistoryRecords(records) {
  const db = await openHistoryDb();
  if (!db) {
    saveHistoryToMemory(records);
    return;
  }
  await new Promise((resolve, reject) => {
    const tx = db.transaction(HISTORY_STORE, 'readwrite');
    const store = tx.objectStore(HISTORY_STORE);
    records.forEach((record) => store.put(record));
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}

async function pruneHistoryRecords() {
  const now = Date.now();
  const lastPrune = Number(localStorage.getItem('gssm-history-pruned-at')) || 0;
  if (now - lastPrune < 60 * 60 * 1000) return;
  localStorage.setItem('gssm-history-pruned-at', String(now));
  const db = await openHistoryDb();
  if (!db) return;
  const cutoff = now - HISTORY_RETENTION_MS;
  await new Promise((resolve, reject) => {
    const tx = db.transaction(HISTORY_STORE, 'readwrite');
    const index = tx.objectStore(HISTORY_STORE).index('ts');
    const request = index.openCursor(IDBKeyRange.upperBound(cutoff, true));
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) return;
      cursor.delete();
      cursor.continue();
    };
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}

async function readHistoryRecords(contract, since) {
  const db = await openHistoryDb();
  if (!db) {
    return (historyState.memory.get(contract) || [])
      .filter((item) => item.ts >= since)
      .sort((a, b) => a.ts - b.ts);
  }
  return new Promise((resolve, reject) => {
    const tx = db.transaction(HISTORY_STORE, 'readonly');
    const index = tx.objectStore(HISTORY_STORE).index('contractTs');
    const range = IDBKeyRange.bound([contract, since], [contract, Date.now() + HISTORY_BUCKET_MS]);
    const request = index.getAll(range);
    request.onsuccess = () => resolve(request.result.sort((a, b) => a.ts - b.ts));
    request.onerror = () => reject(request.error);
  });
}

async function recordHistorySnapshot(bucket, view) {
  const pendingKey = view + ':' + bucket;
  historyState.pendingBuckets.delete(pendingKey);
  const rows = rowsForView(view).filter((row) =>
    row.openArbPct !== null && row.closeArbPct !== null &&
    isFinite(row.openArbPct) && isFinite(row.closeArbPct));
  if (!rows.length) return;
  try {
    await saveHistoryRecords(rows.map((row) => historyRecord(row, bucket)));
    historyState.savedBuckets[view] = bucket;
    pruneHistoryRecords().catch((e) => console.warn('历史价差清理失败', e));
    if (historyState.activeContract && !$('historyModal').classList.contains('hidden')) {
      renderHistoryModal();
    }
  } catch (e) {
    console.warn('历史价差保存失败', e);
  }
}

function maybeRecordHistory() {
  const bucket = Math.floor(Date.now() / HISTORY_BUCKET_MS) * HISTORY_BUCKET_MS;
  const view = state.view;
  const pendingKey = view + ':' + bucket;
  if (historyState.savedBuckets[view] === bucket || historyState.pendingBuckets.has(pendingKey)) return;
  historyState.pendingBuckets.add(pendingKey);
  clearTimeout(historyState.recordTimers[view]);
  historyState.recordTimers[view] = setTimeout(() => recordHistorySnapshot(bucket, view), 2500);
}

function formatHistoryPct(value) {
  if (value === null || value === undefined || !isFinite(value)) return '—';
  return (value > 0 ? '+' : '') + fmt(value) + '%';
}

function formatHistoryTime(ts, withDate = false) {
  return new Date(ts).toLocaleString('zh-CN', {
    month: withDate ? '2-digit' : undefined,
    day: withDate ? '2-digit' : undefined,
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

function liveHistoryRecord(contract) {
  const row = (lastSnapshot || []).find((item) => item.contract === contract);
  if (!row || row.openArbPct === null || row.closeArbPct === null) return null;
  return historyRecord(row, Date.now());
}

async function openHistoryModal(contract, trigger) {
  const row = (lastSnapshot || []).find((item) => item.contract === contract);
  if (!row) return;
  historyState.activeContract = contract;
  historyState.previousFocus = trigger || document.activeElement;
  historyState.previousContract = contract;
  $('historyTitle').textContent = contract.replace('_USDT', '') + '/USDT · ' + row.name;
  $('historySubtitle').textContent = 'Gate 盘口 vs ' + (row.mktMkt || row.market || '真实市场') + '盘口';
  $('historyModal').classList.remove('hidden');
  document.body.classList.add('modal-open');
  $('historyClose').focus();
  await renderHistoryModal();
}

function closeHistoryModal() {
  $('historyModal').classList.add('hidden');
  document.body.classList.remove('modal-open');
  hideHistoryTooltip();
  const previous = historyState.previousFocus;
  const previousContract = historyState.previousContract;
  historyState.previousFocus = null;
  historyState.previousContract = null;
  if (previous && document.contains(previous)) previous.focus();
  else if (previousContract) {
    const currentButton = document.querySelector(`.history-btn[data-history="${previousContract}"]`);
    if (currentButton) currentButton.focus();
  }
}

async function renderHistoryModal() {
  const contract = historyState.activeContract;
  if (!contract) return;
  const token = ++historyState.renderToken;
  const now = Date.now();
  const since = now - historyState.rangeHours * 60 * 60 * 1000;
  $('historyMeta').textContent = '正在读取…';
  try {
    let points = await readHistoryRecords(contract, since);
    if (token !== historyState.renderToken || historyState.activeContract !== contract) return;
    const live = liveHistoryRecord(contract);
    if (live) {
      const last = points[points.length - 1];
      if (last && Math.floor(last.ts / HISTORY_BUCKET_MS) === Math.floor(live.ts / HISTORY_BUCKET_MS)) {
        points[points.length - 1] = live;
      } else {
        points.push(live);
      }
    }
    points = points.filter((point) => point.ts >= since).sort((a, b) => a.ts - b.ts);
    renderHistorySummary(points);
    renderHistoryChart(points, since, now);
  } catch (e) {
    console.error('历史价差读取失败', e);
    $('historyMeta').textContent = '读取失败';
    $('historyChart').classList.add('hidden');
    $('historyEmpty').classList.remove('hidden');
    $('historyEmpty').querySelector('strong').textContent = '历史记录读取失败';
    $('historyEmpty').querySelector('span').textContent = '请稍后重试，当前实时行情不受影响。';
  }
}

function renderHistorySummary(points) {
  const latest = points[points.length - 1];
  const openValues = points.map((point) => point.openArbPct).filter(isFinite);
  const closeValues = points.map((point) => point.closeArbPct).filter(isFinite);
  $('histOpenNow').textContent = latest ? formatHistoryPct(latest.openArbPct) : '—';
  $('histOpenHigh').textContent = openValues.length ? formatHistoryPct(Math.max(...openValues)) : '—';
  $('histCloseNow').textContent = latest ? formatHistoryPct(latest.closeArbPct) : '—';
  $('histCloseLow').textContent = closeValues.length ? formatHistoryPct(Math.min(...closeValues)) : '—';
  if (!points.length) {
    $('historyMeta').textContent = '暂无采样点';
  } else {
    $('historyMeta').textContent = points.length + ' 个采样点 · ' +
      formatHistoryTime(points[0].ts, true) + ' — ' + formatHistoryTime(points[points.length - 1].ts, true);
  }
}

function historyPath(points, key, xFor, yFor) {
  let previousTs = null;
  return points.map((point) => {
    const command = previousTs === null || point.ts - previousTs > HISTORY_BUCKET_MS * 3 ? 'M' : 'L';
    previousTs = point.ts;
    return command + xFor(point.ts).toFixed(2) + ' ' + yFor(point[key]).toFixed(2);
  }).join(' ');
}

function renderHistoryChart(points, since, now) {
  historyState.chartPoints = [];
  historyState.chartSince = since;
  historyState.chartNow = now;
  hideHistoryTooltip();

  const chart = $('historyChart');
  const empty = $('historyEmpty');
  if (points.length < 2) {
    chart.classList.add('hidden');
    empty.classList.remove('hidden');
    empty.querySelector('strong').textContent = points.length ? '已记录当前价差' : '还没有历史记录';
    empty.querySelector('span').textContent = points.length
      ? '再获得一个 5 分钟采样点后即可绘制曲线。'
      : '保持页面打开，系统会为当前视图每 5 分钟保存一个盘口价差点。';
    return;
  }
  chart.classList.remove('hidden');
  empty.classList.add('hidden');

  const width = Math.max($('historyChartWrap').clientWidth || 960, 320), height = 340;
  historyState.chartWidth = width;
  chart.setAttribute('viewBox', `0 0 ${width} ${height}`);
  const pad = { left: 62, right: 20, top: 20, bottom: 42 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;
  const values = [0];
  points.forEach((point) => values.push(point.openArbPct, point.closeArbPct));
  let minY = Math.min(...values), maxY = Math.max(...values);
  const yPadding = Math.max((maxY - minY) * 0.12, 0.1);
  minY -= yPadding;
  maxY += yPadding;
  const xFor = (ts) => pad.left + ((ts - since) / Math.max(now - since, 1)) * plotW;
  const yFor = (value) => pad.top + ((maxY - value) / Math.max(maxY - minY, 0.01)) * plotH;

  const grid = [];
  for (let i = 0; i <= 4; i += 1) {
    const y = pad.top + plotH * i / 4;
    const value = maxY - (maxY - minY) * i / 4;
    grid.push(`<line class="chart-grid-line" x1="${pad.left}" x2="${width - pad.right}" y1="${y}" y2="${y}"></line>`);
    grid.push(`<text class="chart-axis-label" x="${pad.left - 10}" y="${y + 4}" text-anchor="end">${fmt(value)}%</text>`);
  }
  [since, since + (now - since) / 2, now].forEach((ts, index) => {
    const x = xFor(ts);
    const anchor = index === 0 ? 'start' : index === 2 ? 'end' : 'middle';
    grid.push(`<text class="chart-axis-label" x="${x}" y="${height - 12}" text-anchor="${anchor}">${formatHistoryTime(ts, historyState.rangeHours > 4)}</text>`);
  });
  $('historyGrid').innerHTML = grid.join('');
  $('historyZeroLine').setAttribute('d', `M${pad.left} ${yFor(0)} L${width - pad.right} ${yFor(0)}`);
  $('historyOpenPath').setAttribute('d', historyPath(points, 'openArbPct', xFor, yFor));
  $('historyClosePath').setAttribute('d', historyPath(points, 'closeArbPct', xFor, yFor));
  historyState.chartPoints = points.map((point) => ({
    ...point,
    x: xFor(point.ts),
    openY: yFor(point.openArbPct),
    closeY: yFor(point.closeArbPct),
  }));
}

function hideHistoryTooltip() {
  $('historyTooltip').classList.add('hidden');
  $('historyCrosshair').classList.add('hidden');
  $('historyOpenDot').classList.add('hidden');
  $('historyCloseDot').classList.add('hidden');
}

function showHistoryTooltip(event) {
  const points = historyState.chartPoints;
  if (!points.length) return;
  const wrap = $('historyChartWrap');
  const rect = wrap.getBoundingClientRect();
  const svgX = Math.max(0, Math.min(historyState.chartWidth, (event.clientX - rect.left) / rect.width * historyState.chartWidth));
  let nearest = points[0];
  points.forEach((point) => {
    if (Math.abs(point.x - svgX) < Math.abs(nearest.x - svgX)) nearest = point;
  });
  const crosshair = $('historyCrosshair');
  crosshair.setAttribute('x1', nearest.x);
  crosshair.setAttribute('x2', nearest.x);
  crosshair.classList.remove('hidden');
  const openDot = $('historyOpenDot');
  openDot.setAttribute('cx', nearest.x);
  openDot.setAttribute('cy', nearest.openY);
  openDot.classList.remove('hidden');
  const closeDot = $('historyCloseDot');
  closeDot.setAttribute('cx', nearest.x);
  closeDot.setAttribute('cy', nearest.closeY);
  closeDot.classList.remove('hidden');
  const tooltip = $('historyTooltip');
  tooltip.innerHTML = `<b>${formatHistoryTime(nearest.ts, true)}</b><span>开仓 ${formatHistoryPct(nearest.openArbPct)}</span><span>清仓 ${formatHistoryPct(nearest.closeArbPct)}</span>`;
  tooltip.classList.remove('hidden');
  const left = nearest.x / historyState.chartWidth * rect.width;
  tooltip.style.left = Math.max(82, Math.min(rect.width - 82, left)) + 'px';
}

/* ---------------- 过滤 / 排序 ---------------- */

function filteredRows() {
  let rows = lastSnapshot || [];
  if (state.view === 'ashare') rows = rows.filter((r) => r.market === 'A股');
  else if (state.view === 'hk') rows = rows.filter((r) => r.mktMkt === '港股');
  if (state.search) {
    const q = state.search.toLowerCase();
    rows = rows.filter((r) =>
      r.contract.toLowerCase().includes(q) ||
      r.name.toLowerCase().includes(q) ||
      (r.aCode && r.aCode.toLowerCase().includes(q))
    );
  }
  const dir = state.sortDir;
  const k = state.sortKey;
  return rows.slice().sort((a, b) => {
    const va = a[k], vb = b[k];
    if (va === null || va === undefined) return 1;
    if (vb === null || vb === undefined) return -1;
    if (k === 'name') return va.localeCompare(vb, 'zh') * dir;
    return (va - vb) * dir;
  });
}

/* ---------------- 渲染 ---------------- */

const BADGE_CLASS = { 'A股': 'ashare', '港股': 'hk' };
const BADGE_TEXT = { 'A股': 'A股', '港股': '港股' };
const MKT_TEXT = { 'A股': 'A股', '港股': '港股', '美股': '美股' };

function render() {
  if (!lastSnapshot) return;
  renderStats();
  renderTable();
  renderAlerts();
  maybeRecordHistory();
}

function renderStats() {
  const rows = filteredRows();
  const withMkt = rows.filter((r) => r.mktBid !== null);
  $('statCount').textContent = rows.length;
  $('statCountNote').textContent =
    state.view === 'ashare' ? '对标 A 股的合约' :
    state.view === 'hk' ? '对标港股的合约' : '全部监控合约';
  const openBest = withMkt.filter((r) => r.openArbPct !== null).sort((a, b) => b.openArbPct - a.openArbPct)[0];
  $('statOpen').textContent = openBest ? fmt(openBest.openArbPct) + '%' : '—';
  $('statOpenNote').textContent = openBest ? openBest.contract.replace('_USDT', '') + ' ' + openBest.name : '暂无数据';
  const closeBest = withMkt.filter((r) => r.closeArbPct !== null).sort((a, b) => b.closeArbPct - a.closeArbPct)[0];
  $('statClose').textContent = closeBest ? fmt(closeBest.closeArbPct) + '%' : '—';
  $('statCloseNote').textContent = closeBest ? closeBest.contract.replace('_USDT', '') + ' ' + closeBest.name : '暂无数据';
  // 溢价收敛策略：机会只看开仓差价（Gate 溢价时开仓）
  const opp = withMkt.filter((r) => r.openArbPct !== null && r.openArbPct >= state.threshold);
  $('statOver').textContent = opp.length;
  $('statOverNote').textContent = '开仓差价 ≥ ' + state.threshold.toFixed(2) + '%';
  $('fxNote').textContent = marketState.fx.CNY && marketState.fx.HKD
    ? `USD 1:1 · CNY ${marketState.fx.CNY.toFixed(3)} · HKD ${marketState.fx.HKD.toFixed(3)}`
    : 'USD 1:1 · USDT';
}

function arbCell(v, pct) {
  if (v === null || pct === null) return '<td class="num nodepth">—</td>';
  const cls = pct >= state.threshold ? 'arb-big' : pct > 0 ? 'arb-pos' : pct < 0 ? 'arb-neg' : '';
  const sign = pct > 0 ? '+' : '';
  return `<td class="num"><span class="${cls}" title="绝对值 ${fmt(v, 3)} USDT">${sign}${fmt(pct)}%</span></td>`;
}

/* 清仓差价：参考列，只做正负着色，不参与机会高亮 */
function arbCellPlain(v, pct) {
  if (v === null || pct === null) return '<td class="num nodepth">—</td>';
  const cls = pct > 0 ? 'arb-pos' : pct < 0 ? 'arb-neg' : '';
  const sign = pct > 0 ? '+' : '';
  return `<td class="num"><span class="${cls}" title="绝对值 ${fmt(v, 3)} USDT">${sign}${fmt(pct)}%</span></td>`;
}

function renderTable() {
  const tbody = $('tbody');
  const rows = filteredRows();
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="12" class="empty-row">暂无符合条件的合约</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map((r) => {
    const badge = BADGE_CLASS[r.market]
      ? `<span class="badge ${BADGE_CLASS[r.market]}" title="${r.market === 'A股' ? 'A股代码 ' + r.aCode : r.market === '港股' ? '港股代码 ' + r.hkCode : ''}">${BADGE_TEXT[r.market]}</span>` : '';
    // 与合约市场徽章相同时去重（如 A股 合约不再重复显示两个「A股」）
    const mktBadge = r.mktMkt && r.mktMkt !== r.market
      ? `<span class="badge ${r.mktMkt === 'A股' ? 'ashare' : r.mktMkt === '港股' ? 'hk' : 'us'}">${MKT_TEXT[r.mktMkt]}</span>` : '';
    const premBadge = r.prem !== null && Math.abs(r.prem) >= 0.3
      ? `<span class="prem ${r.prem > 0 ? 'prem-up' : 'prem-down'}" title="Gate 中间价相对市场${r.prem > 0 ? '溢价' : '折价'}">${r.prem > 0 ? '溢' : '折'}${fmt(Math.abs(r.prem))}%</span>` : '';
    const approxTip = r.mktApprox ? ' <span class="dim" title="非交易时段或美股无盘口，取最新价近似">≈</span>' : '';
    const mktBidCell = r.mktBid !== null
      ? `<td class="num">${fmt(r.mktBid)}${approxTip}</td><td class="num">${fmt(r.mktAsk)}${approxTip}</td>`
      : '<td class="num nodepth" colspan="2" title="腾讯行情无此标的（未上市/非 A股·港股·美股上市）">无对标行情</td>';
    const depthCell = r.spread === null
      ? '<td class="num nodepth" colspan="2" title="Gate 暂无 best bid / ask 盘口">无 Gate 盘口</td>'
      : `<td class="num">${fmt(r.bid)}</td><td class="num">${fmt(r.ask)}</td>`;
    const chgCls = r.chg > 0 ? 'up' : r.chg < 0 ? 'down' : '';
    return `<tr data-symbol="${r.contract}"${r.fresh ? ' class="flash-new"' : ''}>
      <td class="symbol">${r.contract.replace('_USDT', '')}<span class="dim">/USDT</span></td>
      <td class="name">${r.name}${badge}${mktBadge}${premBadge}</td>
      ${arbCell(r.openArb, r.openArbPct)}
      ${arbCellPlain(r.closeArb, r.closeArbPct)}
      ${mktBidCell}
      ${depthCell}
      <td class="num ${chgCls}">${r.chg > 0 ? '+' : ''}${fmt(r.chg, 2)}%</td>
      <td class="num" title="24h 成交额（USDT）">${fmtVol(r.vol)}</td>
      <td class="num">${fmt(r.oi, 0)}</td>
      <td class="history-cell"><button type="button" class="history-btn" data-history="${r.contract}" aria-label="查看 ${r.contract} 的历史价差">查看</button></td>
    </tr>`;
  }).join('');
}

/* ---------------- 告警 ---------------- */

function renderAlerts() {
  const opp = (lastSnapshot || []).filter((r) =>
    r.openArbPct !== null && r.openArbPct >= state.threshold);
  const banner = $('alertBanner');
  if (!opp.length) { banner.classList.add('hidden'); return; }
  const top = opp.slice().sort((a, b) => b.openArbPct - a.openArbPct).slice(0, 5);
  const list = top.map((r) =>
    `<b>${r.contract.replace('_USDT', '')}</b> ${r.name} 开仓差价 <b>${fmt(r.openArbPct)}%</b>`
  ).join('　');
  banner.innerHTML = `<span class="close" onclick="this.parentElement.classList.add('hidden')">✕</span>
    🎯 <b>${opp.length}</b> 个开仓机会（Gate 溢价）≥ ${state.threshold.toFixed(2)}%：<br>${list}`;
  banner.classList.remove('hidden');

  if ('Notification' in window && Notification.permission === 'granted') {
    top.forEach((r) => {
      if (!state.notified.has(r.contract)) {
        state.notified.add(r.contract);
        new Notification('开仓机会 ' + r.contract.replace('_USDT', ''), {
          body: `${r.name}：开仓差价 ${fmt(r.openArbPct)}%（Gate买一 ${fmt(r.bid)} vs 市场卖一 ${fmt(r.mktAsk)}）`,
          tag: r.contract,
        });
      }
    });
  }
}

/* ---------------- 事件绑定 ---------------- */

function bindEvents() {
  syncSortIndicator();
  $('viewSeg').addEventListener('click', (e) => {
    const btn = e.target.closest('.seg-btn');
    if (!btn || state.view === btn.dataset.view) return;
    state.view = btn.dataset.view;
    document.querySelectorAll('.seg-btn').forEach((b) => b.classList.toggle('active', b === btn));
    if (state.mode === 'ws' && wsState.ws && wsState.connected) subscribeView();
    fetchMarketQuotes();
    render();
  });

  $('searchInput').addEventListener('input', (e) => { state.search = e.target.value.trim(); render(); });

  $('thresholdSel').addEventListener('change', (e) => { state.threshold = parseFloat(e.target.value); render(); });

  $('intervalSel').addEventListener('change', (e) => {
    state.interval = parseInt(e.target.value, 10);
    if (state.mode === 'local') restartTimer();
    restartMarketTimer();
  });

  $('autoRefresh').addEventListener('change', (e) => {
    state.autoRefresh = e.target.checked;
    if (state.mode === 'local') restartTimer();
    restartMarketTimer();
  });

  $('refreshBtn').addEventListener('click', () => {
    if (state.mode === 'local') fetchTickersLocal();
    else { subscribeView(); toast('已重新订阅行情流'); }
    fetchMarketQuotes();
  });

  $('themeBtn').addEventListener('click', () => {
    state.theme = state.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = state.theme;
    localStorage.setItem('gssm-theme', state.theme);
  });

  $('notifyBtn').addEventListener('click', async () => {
    if (!('Notification' in window)) return toast('当前浏览器不支持通知');
    if (Notification.permission === 'granted') return toast('通知已开启');
    const p = await Notification.requestPermission();
    toast(p === 'granted' ? '通知已开启，价差机会超阈值时将提醒' : '通知权限被拒绝');
  });

  $('tbody').addEventListener('click', (e) => {
    const btn = e.target.closest('.history-btn');
    if (!btn) return;
    openHistoryModal(btn.dataset.history, btn);
  });

  $('historyClose').addEventListener('click', closeHistoryModal);
  $('historyModal').addEventListener('click', (e) => {
    if (e.target === $('historyModal')) closeHistoryModal();
  });
  $('historyRange').addEventListener('click', (e) => {
    const btn = e.target.closest('.seg-btn[data-hours]');
    if (!btn) return;
    historyState.rangeHours = Number(btn.dataset.hours);
    $('historyRange').querySelectorAll('.seg-btn').forEach((item) => item.classList.toggle('active', item === btn));
    renderHistoryModal();
  });
  $('historyChartWrap').addEventListener('pointermove', showHistoryTooltip);
  $('historyChartWrap').addEventListener('pointerleave', hideHistoryTooltip);
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !$('historyModal').classList.contains('hidden')) closeHistoryModal();
  });

  document.querySelectorAll('thead th[data-key]').forEach((th) => {
    th.addEventListener('click', () => {
      const k = th.dataset.key;
      if (state.sortKey === k) state.sortDir *= -1;
      else { state.sortKey = k; state.sortDir = -1; }
      syncSortIndicator();
      render();
    });
  });

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && state.mode === 'ws' && (!wsState.ws || wsState.ws.readyState !== WebSocket.OPEN)) connectWS();
  });
}

function syncSortIndicator() {
  document.querySelectorAll('thead th').forEach((x) => x.classList.remove('sorted'));
  document.querySelectorAll('thead th .arrow').forEach((a) => a.remove());
  const th = document.querySelector('thead th[data-key="' + state.sortKey + '"]');
  if (th) {
    th.classList.add('sorted');
    const span = document.createElement('span');
    span.className = 'arrow';
    span.textContent = state.sortDir === -1 ? '▼' : '▲';
    th.appendChild(span);
  }
}

function restartTimer() {
  clearInterval(state.timer);
  if (!state.autoRefresh) return;
  state.timer = setInterval(() => fetchTickersLocal(), state.interval * 1000);
}

function restartMarketTimer() {
  clearInterval(marketState.fxTimer);
  if (!state.autoRefresh) return;
  marketState.fxTimer = setInterval(() => fetchMarketQuotes(), state.interval * 1000);
}

function toast(msg) {
  const t = $('toast');
  t.textContent = msg;
  t.classList.remove('hidden');
  clearTimeout(t._timer);
  t._timer = setTimeout(() => t.classList.add('hidden'), 2500);
}

/* ---------------- 启动 ---------------- */

(async function init() {
  document.documentElement.dataset.theme = state.theme;
  await loadNames();
  const cntAshare = Object.values(STOCK_NAMES).filter((v) => v.market === 'A股').length;
  const cntHk = Object.values(STOCK_NAMES).filter((v) => v.mkt === '港股').length;
  $('cntAshare').textContent = cntAshare;
  $('cntHk').textContent = cntHk;
  $('cntAll').textContent = Object.keys(STOCK_NAMES).length;
  bindEvents();
  await loadFx();
  render();

  state.mode = await detectMode();
  if (state.mode === 'local') {
    $('statStatus').textContent = '本地代理';
    await fetchTickersLocal();
    restartTimer();
  } else {
    connectWS();
  }
  fetchMarketQuotes();
  restartMarketTimer();
  if ('Notification' in window && Notification.permission === 'granted') {
    $('notifyBtn').textContent = '🔔 已开启';
  }
})();

async function loadNames() {
  try {
    const res = await fetch('stock-names.json');
    STOCK_NAMES = await res.json();
  } catch (e) {
    STOCK_NAMES = {};
  }
}

function setStatus(text, off) {
  const el = $('statStatus');
  el.textContent = text;
  el.closest('.stat-card').classList.toggle('off', off);
}

/* 顶栏高度同步给 sticky 表头（随窗口宽度变化） */
function syncTopbarHeight() {
  const tb = document.querySelector('.topbar');
  if (tb) document.documentElement.style.setProperty('--topbar-h', tb.offsetHeight + 'px');
}
window.addEventListener('resize', syncTopbarHeight);
window.addEventListener('load', syncTopbarHeight);
