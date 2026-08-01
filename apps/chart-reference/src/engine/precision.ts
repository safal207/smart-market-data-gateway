export interface PricePrecision {
  readonly precision: number;
  readonly minMove: number;
}

export interface PricePrecisionOptions {
  readonly tickSize?: number;
  readonly observedPrices?: readonly number[];
  readonly fallbackPrecision?: number;
  readonly maximumPrecision?: number;
}

export function derivePricePrecision(
  options: PricePrecisionOptions = {},
): PricePrecision {
  const maximum = boundedInteger(options.maximumPrecision ?? 8, 0, 12);
  const fallback = boundedInteger(options.fallbackPrecision ?? 2, 0, maximum);
  if (options.tickSize != null) {
    if (!Number.isFinite(options.tickSize) || options.tickSize <= 0) {
      throw new RangeError("tickSize must be a positive finite number");
    }
    return Object.freeze({
      precision: Math.min(maximum, decimalPlaces(options.tickSize)),
      minMove: options.tickSize,
    });
  }

  const observed = (options.observedPrices ?? []).filter(
    (price) => Number.isFinite(price) && price > 0,
  );
  const precision =
    observed.length === 0
      ? fallback
      : Math.min(maximum, Math.max(fallback, ...observed.map(decimalPlaces)));
  return Object.freeze({ precision, minMove: 10 ** -precision });
}

export function formatPrice(value: number, precision: PricePrecision | number): string {
  if (!Number.isFinite(value)) return "—";
  const digits =
    typeof precision === "number" ? precision : precision.precision;
  const bounded = boundedInteger(digits, 0, 12);
  const normalized = Object.is(value, -0) ? 0 : value;
  return normalized.toFixed(bounded);
}

export function decimalPlaces(value: number): number {
  if (!Number.isFinite(value)) return 0;
  const text = Math.abs(value).toString().toLowerCase();
  const [coefficient, exponentText] = text.split("e");
  const exponent = exponentText == null ? 0 : Number(exponentText);
  const fractionLength = coefficient?.split(".")[1]?.length ?? 0;
  return Math.max(0, fractionLength - exponent);
}

function boundedInteger(value: number, minimum: number, maximum: number): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new RangeError(`precision must be an integer from ${minimum} to ${maximum}`);
  }
  return value;
}
