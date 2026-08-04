import { useEffect, useState, type FormEvent } from "react";

import type { ChartTheme } from "../../chart";
import type { TimeframeOption, WorkspaceSourceMode } from "./types";

export interface InstrumentControlProps {
  readonly symbol: string;
  readonly symbolOptions: readonly string[];
  readonly timeframe: string;
  readonly timeframeOptions: readonly TimeframeOption[];
  readonly theme: ChartTheme;
  readonly sourceMode: WorkspaceSourceMode;
  readonly gatewayToken: string;
  readonly paused: boolean;
  readonly onSelectSymbol: (symbol: string) => void;
  readonly onSelectTimeframe: (timeframe: string) => void;
  readonly onSelectTheme: (theme: ChartTheme) => void;
  readonly onSelectSourceMode: (mode: WorkspaceSourceMode) => void;
  readonly onGatewayTokenChange: (token: string) => void;
  readonly onTogglePaused: () => void;
  readonly onReconnect: () => void;
}

export function InstrumentControl({
  symbol,
  symbolOptions,
  timeframe,
  timeframeOptions,
  theme,
  sourceMode,
  gatewayToken,
  paused,
  onSelectSymbol,
  onSelectTimeframe,
  onSelectTheme,
  onSelectSourceMode,
  onGatewayTokenChange,
  onTogglePaused,
  onReconnect,
}: InstrumentControlProps) {
  const [draftSymbol, setDraftSymbol] = useState(symbol);

  useEffect(() => setDraftSymbol(symbol), [symbol]);

  function submit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const normalized = draftSymbol.trim().toUpperCase();
    if (normalized !== "") {
      onSelectSymbol(normalized);
    }
  }

  return (
    <section className="control-panel" aria-label="Chart controls">
      <form className="instrument-form" onSubmit={submit}>
        <div className="field field--symbol">
          <label htmlFor="instrument-symbol">Instrument</label>
          <input
            id="instrument-symbol"
            list="instrument-options"
            value={draftSymbol}
            onChange={(event) => setDraftSymbol(event.currentTarget.value)}
            pattern="[A-Za-z0-9._:-]{1,32}"
            maxLength={32}
            autoCapitalize="characters"
            autoComplete="off"
            spellCheck={false}
          />
          <datalist id="instrument-options">
            {symbolOptions.map((option) => (
              <option value={option} key={option} />
            ))}
          </datalist>
        </div>
        <button className="button button--primary" type="submit">
          Load
        </button>
      </form>

      <div className="field">
        <label htmlFor="timeframe">Timeframe</label>
        <select
          id="timeframe"
          value={timeframe}
          disabled={paused}
          aria-describedby={paused ? "timeframe-paused-hint" : undefined}
          onChange={(event) => onSelectTimeframe(event.currentTarget.value)}
        >
          {timeframeOptions.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        {paused ? (
          <p id="timeframe-paused-hint" className="field-hint">
            Resume the display before changing timeframe.
          </p>
        ) : null}
      </div>

      <button
        className="button button--secondary"
        type="button"
        aria-pressed={paused}
        onClick={onTogglePaused}
      >
        {paused ? "Resume display" : "Pause display"}
      </button>

      <button
        className="button button--icon"
        type="button"
        aria-label={`Use ${theme === "dark" ? "light" : "dark"} theme`}
        onClick={() => onSelectTheme(theme === "dark" ? "light" : "dark")}
      >
        <span aria-hidden="true">{theme === "dark" ? "☀" : "☾"}</span>
      </button>

      <details className="source-settings">
        <summary>Data source</summary>
        <div className="source-settings__body">
          <fieldset>
            <legend>Mode</legend>
            <label>
              <input
                type="radio"
                name="source-mode"
                value="replay"
                checked={sourceMode === "replay"}
                onChange={() => onSelectSourceMode("replay")}
              />
              Synthetic replay
            </label>
            <label>
              <input
                type="radio"
                name="source-mode"
                value="gateway"
                checked={sourceMode === "gateway"}
                onChange={() => onSelectSourceMode("gateway")}
              />
              Local gateway
            </label>
          </fieldset>
          {sourceMode === "gateway" ? (
            <div className="field source-settings__token">
              <label htmlFor="gateway-token">Local gateway token</label>
              <input
                id="gateway-token"
                type="password"
                value={gatewayToken}
                onChange={(event) => onGatewayTokenChange(event.currentTarget.value)}
                autoComplete="off"
                spellCheck={false}
              />
              <p className="field-hint">Kept in memory for this tab. Never saved to browser storage.</p>
              <button className="button button--secondary" type="button" onClick={onReconnect}>
                Connect with token
              </button>
            </div>
          ) : (
            <p className="field-hint">Synthetic deterministic data; safe for screenshots and tests.</p>
          )}
        </div>
      </details>
    </section>
  );
}
