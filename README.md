# Gate × 真实市场 · 开平仓价差监控

从 workbuddy（agentos）预览站移植到 GitHub Pages 的静态版本。

- 在线地址：https://surfacebuaa-maker.github.io/gate-spread-radar/
- 部署方式：GitHub Pages（静态托管，无后端）

## 工作原理

- Gate.io 合约盘口：浏览器直连 Gate 期货 WebSocket（`wss://fx-ws.gateio.ws/v4/ws/usdt/`），订阅 `futures.tickers` 与 `futures.book_ticker`（307 个股票合约）
- 真实市场行情：腾讯行情接口 `qt.gtimg.cn`（A股/港股实时盘口、美股最新价，CORS 可用）
- 汇率换算：`open.er-api.com`（USD 基准，浏览器本地缓存 6 小时）
- 无 `/api` 后端时自动进入 WS 模式，因此可在任意静态托管运行
- 历史价差：按当前视图每 5 分钟保存一次开仓/清仓盘口价差到浏览器 IndexedDB，可查看 4 小时、1 天、3 天；滚动保留 3 天

> 历史记录从当前浏览器首次采集开始，页面关闭或设备休眠期间不会补录，也不会同步到其他设备。

## 本地编辑与预览

直接编辑 `index.html` / `style.css` / `app.js` / `stock-names.json` 即可。
本地预览可用任意静态文件服务器（如 `python -m http.server 8000`）打开。

## 部署

```bash
git add . && git commit -m "update"
git push origin main
```

GitHub Pages 已配置为从 main 分支根目录发布，推送后约 1 分钟生效。
