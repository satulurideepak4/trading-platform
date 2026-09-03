export type Side = "BUY" | "SELL";
export type OrderType = "LIMIT" | "MARKET";

export interface BookLevel { price: number; quantity: number; orderCount: number }
export interface OrderBook { symbol: string; bids: BookLevel[]; asks: BookLevel[] }
export interface Position { strategyId: string; symbol: string; netQuantity: number; averageEntryPrice: number | null; markPrice: number | null; realizedPnl: number; unrealizedPnl: number; totalPnl: number; executionCount: number; updatedAt: string }
export interface Execution { executionId: number; symbol: string; side: Side; price: number; quantity: number; liquidity: "MAKER" | "TAKER"; occurredAt: string }
export interface Exposure { symbol: string; netPosition: number; workingBuyQuantity: number; workingSellQuantity: number; longExposure: number; shortExposure: number }
export interface Risk { accountId: string; openOrders: number; exposures: Exposure[] }
export interface MarketUpdate { symbol: string; type: "BID" | "ASK" | "TRADE"; price: number; quantity: number; exchangeSequence: number; receivedAt: string; outcome: string; gapSize: number }
export interface OrderAck { orderId: number; status: string; remainingQuantity: number; duplicate?: boolean; executions?: Execution[] }

export class GatewayClient {
  constructor(private readonly apiKey: string) {}

  private async request<T>(path: string, init: RequestInit = {}, authenticated = true): Promise<T> {
    const response = await fetch(`/api${path}`, {
      ...init,
      headers: { "Content-Type": "application/json", ...(authenticated ? { "X-Api-Key": this.apiKey } : {}), ...init.headers }
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(body.message || body.reason || `Gateway returned ${response.status}`);
    }
    return response.status === 204 ? (undefined as T) : response.json() as Promise<T>;
  }

  health() { return this.request<{ status: string }>("/actuator/health", {}, false); }
  book(symbol: string) { return this.request<OrderBook>(`/orderbook/${symbol}?depth=12`); }
  positions() { return this.request<Position[]>("/positions"); }
  executions() { return this.request<Execution[]>("/executions?limit=20"); }
  risk() { return this.request<Risk>("/risk/exposure"); }
  submit(input: { clientOrderId: string; symbol: string; side: Side; type: OrderType; quantity: number; price?: number }) {
    return this.request<OrderAck>("/orders", { method: "POST", body: JSON.stringify(input) });
  }
  cancel(orderId: number) { return this.request<OrderAck>(`/orders/${orderId}`, { method: "DELETE" }); }
}
