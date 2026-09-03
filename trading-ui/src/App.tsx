import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { GatewayClient, type Execution, type MarketUpdate, type OrderAck, type OrderBook, type Position, type Risk, type Side } from "./api";

const symbols = ["AAPL", "MSFT", "NVDA", "TSLA"];
const ticks = (value: number | null | undefined) => value == null ? "—" : (value / 100).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const signed = (value: number) => `${value >= 0 ? "+" : ""}${ticks(value)}`;

function App() {
  const [apiKey, setApiKey] = useState(() => sessionStorage.getItem("aurum-api-key") || "local-dev-key");
  const [connected, setConnected] = useState(false);
  const [selected, setSelected] = useState("AAPL");
  const [book, setBook] = useState<OrderBook | null>(null);
  const [market, setMarket] = useState<Record<string, MarketUpdate>>({});
  const [positions, setPositions] = useState<Position[]>([]);
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [risk, setRisk] = useState<Risk | null>(null);
  const [notice, setNotice] = useState("Connect a local gateway to begin trading.");
  const [lastOrder, setLastOrder] = useState<OrderAck | null>(null);
  const [side, setSide] = useState<Side>("BUY");
  const [orderType, setOrderType] = useState<"LIMIT" | "MARKET">("LIMIT");
  const [quantity, setQuantity] = useState("100");
  const [price, setPrice] = useState("190.00");
  const socket = useRef<WebSocket | null>(null);

  const client = useMemo(() => new GatewayClient(apiKey), [apiKey]);
  const refresh = useCallback(async () => {
    try {
      const [currentBook, currentPositions, currentExecutions, currentRisk] = await Promise.all([
        client.book(selected), client.positions(), client.executions(), client.risk()
      ]);
      setBook(currentBook); setPositions(currentPositions); setExecutions(currentExecutions); setRisk(currentRisk); setConnected(true);
      setNotice("Gateway connected. Portfolio data is a durable asynchronous projection.");
    } catch (error) {
      setConnected(false); setNotice(error instanceof Error ? error.message : "Gateway is unavailable");
    }
  }, [client, selected]);

  useEffect(() => { refresh(); const timer = window.setInterval(refresh, 5000); return () => window.clearInterval(timer); }, [refresh]);
  useEffect(() => {
    socket.current?.close();
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${scheme}://${location.host}/marketdata`);
    socket.current = ws;
    ws.onopen = () => ws.send(JSON.stringify({ type: "subscribe", symbols }));
    ws.onmessage = event => { try { const update = JSON.parse(event.data) as MarketUpdate; setMarket(previous => ({ ...previous, [update.symbol]: update })); } catch { /* binary mode is intentionally not rendered in a browser client */ } };
    return () => ws.close();
  }, []);
  useEffect(() => { refresh(); }, [selected, refresh]);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    const quantityTicks = Number(quantity);
    const priceTicks = Math.round(Number(price) * 100);
    if (!Number.isInteger(quantityTicks) || quantityTicks <= 0 || (orderType === "LIMIT" && priceTicks <= 0)) { setNotice("Quantity and limit price must be positive."); return; }
    try {
      const ack = await client.submit({ clientOrderId: `ui-${crypto.randomUUID()}`, symbol: selected, side, type: orderType, quantity: quantityTicks, ...(orderType === "LIMIT" ? { price: priceTicks } : {}) });
      setLastOrder(ack); setNotice(`Order ${ack.orderId} ${ack.status.toLowerCase()}${ack.duplicate ? " (idempotent replay)" : ""}.`); await refresh();
    } catch (error) { setNotice(error instanceof Error ? error.message : "Order rejected"); }
  };
  const cancel = async () => { if (!lastOrder) return; try { const ack = await client.cancel(lastOrder.orderId); setLastOrder(ack); setNotice(`Order ${ack.orderId} cancelled.`); await refresh(); } catch (error) { setNotice(error instanceof Error ? error.message : "Cancellation failed"); } };
  const totalPnl = positions.reduce((total, item) => total + item.totalPnl, 0);
  const mid = book?.bids[0] && book.asks[0] ? Math.round((book.bids[0].price + book.asks[0].price) / 2) : market[selected]?.price;

  return <main className="terminal">
    <header className="topbar"><div className="brand"><span className="brand-mark">A</span><span>AURUM</span><small>EXECUTION WORKSTATION</small></div><div className="system-status"><span className={connected ? "pulse online" : "pulse"}/>{connected ? "GATEWAY LIVE" : "GATEWAY OFFLINE"}<span className="clock">{new Date().toLocaleTimeString()}</span></div></header>
    <section className="notice" aria-live="polite"><span>●</span>{notice}<button onClick={refresh}>Refresh state</button></section>
    <div className="workspace">
      <aside className="watchlist panel"><div className="panel-title">Market watch <span>LIVE</span></div>{symbols.map(symbol => <button className={`watch-row ${selected === symbol ? "selected" : ""}`} key={symbol} onClick={() => setSelected(symbol)}><strong>{symbol}</strong><span>{ticks(market[symbol]?.price)}</span><small>{market[symbol]?.outcome || "Awaiting feed"}</small></button>)}</aside>
      <section className="center"><div className="instrument-head"><div><span className="eyebrow">NASDAQ / EQUITY</span><h1>{selected}</h1></div><div className="last-price">{ticks(mid)}<small>{market[selected]?.type || "MARK"} · seq {market[selected]?.exchangeSequence || "—"}</small></div></div>
        <div className="book-grid"><Depth title="Asks" rows={book?.asks || []} side="ask"/><Depth title="Bids" rows={book?.bids || []} side="bid"/></div>
        <div className="panel tape"><div className="panel-title">Execution tape <span>{executions.length} RECENT</span></div>{executions.length ? executions.slice(0, 7).map(fill => <div className="tape-row" key={fill.executionId}><span className={fill.side === "BUY" ? "buy" : "sell"}>{fill.side}</span><strong>{fill.symbol}</strong><span>{fill.quantity.toLocaleString()}</span><span>{ticks(fill.price)}</span><small>{fill.liquidity}</small></div>) : <Empty label="No executions for this account"/>}</div>
      </section>
      <aside className="right-rail"><form className="ticket panel" onSubmit={submit}><div className="panel-title">Order ticket <span>ACCOUNT SCOPED</span></div><div className="side-toggle"><button type="button" className={side === "BUY" ? "active buy-bg" : ""} onClick={() => setSide("BUY")}>Buy</button><button type="button" className={side === "SELL" ? "active sell-bg" : ""} onClick={() => setSide("SELL")}>Sell</button></div><label>Order type<select value={orderType} onChange={e => setOrderType(e.target.value as "LIMIT" | "MARKET")}><option>LIMIT</option><option>MARKET</option></select></label><label>Quantity<input inputMode="numeric" value={quantity} onChange={e => setQuantity(e.target.value)}/></label>{orderType === "LIMIT" && <label>Limit price<input inputMode="decimal" value={price} onChange={e => setPrice(e.target.value)}/></label>}<div className="notional">Estimated notional <strong>{ticks((Number(quantity) || 0) * (Math.round(Number(price) * 100) || 0))}</strong></div><button className={`submit ${side === "BUY" ? "buy-bg" : "sell-bg"}`} disabled={!connected}>Submit {side}</button>{lastOrder && <div className="order-state"><span>LAST ORDER</span><strong>#{lastOrder.orderId} · {lastOrder.status}</strong>{["NEW", "PARTIALLY_FILLED"].includes(lastOrder.status) && <button type="button" onClick={cancel}>Cancel order</button>}</div>}</form>
        <div className="panel risk"><div className="panel-title">Pre-trade risk <span>SYNC</span></div><div className="risk-line"><span>Open orders</span><strong>{risk?.openOrders ?? "—"}</strong></div>{risk?.exposures.length ? risk.exposures.map(item => <div className="risk-item" key={item.symbol}><strong>{item.symbol}</strong><span>Net {item.netPosition.toLocaleString()}</span><span>Worst long {item.longExposure.toLocaleString()}</span></div>) : <Empty label="No active exposure"/>}</div></aside>
      <section className="positions panel"><div className="panel-title">Positions <span>DURABLE PROJECTION</span><strong className={totalPnl >= 0 ? "positive" : "negative"}>{signed(totalPnl)} total P&L</strong></div>{positions.length ? <table><thead><tr><th>Symbol</th><th>Strategy</th><th>Position</th><th>Average</th><th>Mark</th><th>Realized</th><th>Unrealized</th><th>Total</th></tr></thead><tbody>{positions.map(row => <tr key={`${row.strategyId}-${row.symbol}`}><td><b>{row.symbol}</b></td><td>{row.strategyId}</td><td className={row.netQuantity >= 0 ? "positive" : "negative"}>{row.netQuantity.toLocaleString()}</td><td>{ticks(row.averageEntryPrice)}</td><td>{ticks(row.markPrice)}</td><td>{signed(row.realizedPnl)}</td><td>{signed(row.unrealizedPnl)}</td><td className={row.totalPnl >= 0 ? "positive" : "negative"}>{signed(row.totalPnl)}</td></tr>)}</tbody></table> : <Empty label="No durable positions for this account"/>}</section>
    </div>
    <footer>All order decisions are made by the gateway. Market data may be delayed, stale, or gapped; verify the live status before acting.</footer>
  </main>;
}

function Depth({ title, rows, side }: { title: string; rows: { price: number; quantity: number; orderCount: number }[]; side: "bid" | "ask" }) { const max = Math.max(...rows.map(row => row.quantity), 1); return <div className="panel depth"><div className="panel-title">{title}<span>{rows.length} LEVELS</span></div><div className="depth-head"><span>Price</span><span>Size</span><span>Orders</span></div>{rows.length ? rows.map(row => <div className={`depth-row ${side}`} key={row.price}><i style={{ width: `${(row.quantity / max) * 100}%` }}/><b>{ticks(row.price)}</b><span>{row.quantity.toLocaleString()}</span><small>{row.orderCount}</small></div>) : <Empty label="No visible liquidity"/>}</div> }
function Empty({ label }: { label: string }) { return <div className="empty">{label}</div> }
export default App;
