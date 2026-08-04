import type { WorkspaceQuote } from "./types";

function formatPrice(value: number | undefined, precision: number): string {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision,
  }).format(value);
}
export interface QuoteStripProps {
  readonly quote: WorkspaceQuote | null;
  readonly precision: number;
}

export function QuoteStrip({ quote, precision }: QuoteStripProps) {
  return (
    <section className="side-card quote-card" aria-labelledby="quote-title">
      <div className="side-card__heading">
        <h2 id="quote-title">Latest quote</h2>
        {quote?.stale === true ? <span className="tag tag--warning">Stale</span> : null}
      </div>
      {quote == null ? (
        <p className="empty-state">Waiting for market data…</p>
      ) : (
        <>
          <p className={`last-price last-price--${quote.direction}`}>
            <span className="visually-hidden">{quote.direction} price:</span>
            {formatPrice(quote.price, precision)}
            <span aria-hidden="true" className="last-price__direction">
              {quote.direction === "up" ? "↗" : quote.direction === "down" ? "↘" : "→"}
            </span>
          </p>
          <dl className="quote-grid">
            <div>
              <dt>Bid</dt>
              <dd>{formatPrice(quote.bid, precision)}</dd>
            </div>
            <div>
              <dt>Ask</dt>
              <dd>{formatPrice(quote.ask, precision)}</dd>
            </div>
            <div>
              <dt>Provider</dt>
              <dd>{quote.provider}</dd>
            </div>
            <div>
              <dt>Instrument</dt>
              <dd>{quote.symbol}</dd>
            </div>
          </dl>
        </>
      )}
    </section>
  );
}
