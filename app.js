/* ============================================================
 * Gate 股票合约 × 真实市场 开平仓价差监控
 *
 * 双数据源模式：
 *   本地模式 : 同源 /api/health 可用 → REST 轮询（完整 307 合约）
 *   WS 模式   : 云端静态托管 → 直连 Gate 期货 WebSocket
 *
 * 真实市场行情（腾讯行情接口，CORS 可用，浏览器直连）：
 *   A股/港股 : 实时买一/卖一盘口（收盘后为最新价）
 *   美股     : 最新价（腾讯不提供美股盘口，标注近似）
 *   汇率     : open.er-api.com（USD 基准，日更缓存）
 *
 * 价差指标（统一"Gate − 市场"视角，Gate 侧为市价单成交价）：
 *   开仓差价% = (Gate买一 − 市场买一×汇率) / (市场买一×汇率)   [市场挂单买 + Gate 市价卖开仓]
 *   清仓差价% = (Gate卖一 − 市场卖一×汇率) / (市场卖一×汇率)   [市场挂单卖 + Gate 市价买平仓]
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
  quotes: new Map(),   // tencentCode -> {bid, ask, last, chg, time, cur}
  fx: { CNY: null, HKD: null, fetchedAt: 0 },
  fxTimer: null,
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
  const codes = currentViewTencentCodes();
  if (!codes.length) return;
  try {
    for (let i = 0; i < codes.length; i += TENCENT_BATCH) {
      const batch = codes.slice(i, i + TENCENT_BATCH);
      const res = await fetch(TENCENT_URL + batch.join(','));
      const buf = await res.arrayBuffer();
      const text = new TextDecoder('gbk').decode(buf);
      for (const line of text.split(';')) {
        const m = line.trim().match(/^v_([\w.]+)="(.*)"$/);
        if (!m) continue;
        const parts = m[2].split('~');
        if (parts.length < 33 || !parts[0]) continue;
        marketState.quotes.set(m[1], {
          last: parseFloat(parts[3]),
          bid: parseFloat(parts[9]),
          ask: parseFloat(parts[19]),
          chg: parseFloat(parts[32]),
          time: parts[30],
        });
      }
    }
    render();
  } catch (e) {
    console.error('市场行情获取失败', e);
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
          approx: info2.mkt === '美股' || q.bid === q.ask,
          time: q.time,
          last: q.last, cur: info2.cur || 'USD',
        };
      }
    }
  }

  let openArbPct = null, openArb = null, closeArbPct = null, closeArb = null;
  let prem = null; // Gate 中间价相对市场中间价的溢/折价（+ 溢价）
  if (hasDepth && mkt) {
    openArb = bid - mkt.bid;                    // 开仓：Gate 市价卖（吃买一）− 市场挂单买（买一）
    openArbPct = mkt.bid > 0 ? (openArb / mkt.bid) * 100 : null;
    closeArb = ask - mkt.ask;                   // 清仓：Gate 市价买（吃卖一）− 市场挂单卖（卖一）
    closeArbPct = mkt.ask > 0 ? (closeArb / mkt.ask) * 100 : null;
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
}

function renderStats() {
  const rows = filteredRows();
  const withMkt = rows.filter((r) => r.mktBid !== null);
  $('statCount').textContent = rows.length;
  $('statCountNote').textContent =
    state.view === 'ashare' ? '对标 A 股的合约' :
    state.view === 'hk' ? '对标港股的合约' : '全部股票合约';
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
  $('fxNote').textContent = marketState.fx.CNY ? `USD/CNY ${marketState.fx.CNY.toFixed(3)}·HKD ${marketState.fx.HKD.toFixed(3)}` : 'USDT';
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
    tbody.innerHTML = '<tr><td colspan="11" class="empty-row">暂无符合条件的合约</td></tr>';
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
      ? '<td class="num nodepth">—</td><td class="num nodepth">—</td>'
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
          body: `${r.name}：开仓差价 ${fmt(r.openArbPct)}%（Gate卖一 ${fmt(r.ask)} vs 市场买一 ${fmt(r.mktBid)}）`,
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
