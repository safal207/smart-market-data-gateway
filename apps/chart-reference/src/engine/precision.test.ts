import { describe, expect, it } from "vitest";
import { decimalPlaces, derivePricePrecision, formatPrice } from "./precision";

describe("price precision", () => {
  it("derives exact precision and minMove from a tick size", () => {
    expect(derivePricePrecision({ tickSize: 0.25 })).toEqual({ precision: 2, minMove: 0.25 });
    expect(derivePricePrecision({ tickSize: 1e-5 })).toEqual({ precision: 5, minMove: 1e-5 });
  });
  it("clamps the implicit fallback when maximum precision is small", () => {
    expect(derivePricePrecision({ maximumPrecision: 0 })).toEqual({ precision: 0, minMove: 1 });
    expect(derivePricePrecision({ maximumPrecision: 1 })).toEqual({ precision: 1, minMove: 0.1 });
  });
  it("infers bounded observed precision and formats deterministically", () => {
    const precision = derivePricePrecision({ observedPrices: [147.4, 147.425], maximumPrecision: 4 });
    expect(precision).toEqual({ precision: 3, minMove: 0.001 });
    expect(formatPrice(147.4, precision)).toBe("147.400");
    expect(formatPrice(Number.NaN, precision)).toBe("—");
    expect(decimalPlaces(1e-7)).toBe(7);
  });
});
