const ars = new Intl.NumberFormat("es-AR", {
  style: "currency",
  currency: "ARS",
  maximumFractionDigits: 0,
});
const percent = new Intl.NumberFormat("es-AR", { style: "percent", maximumFractionDigits: 1 });
const integer = new Intl.NumberFormat("es-AR", { maximumFractionDigits: 0 });
const decimal = new Intl.NumberFormat("es-AR", { maximumFractionDigits: 2 });
const dateTime = new Intl.DateTimeFormat("es-AR", {
  dateStyle: "short",
  timeStyle: "short",
});

export const money = (value: number | null | undefined): string =>
  value === null || value === undefined ? "s/d" : ars.format(value);

export const pct = (value: number | null | undefined): string =>
  value === null || value === undefined ? "s/d" : percent.format(value);

/** Input is a fraction (0.42 -> 42 %). */
export const count = (value: number | null | undefined): string =>
  value === null || value === undefined ? "s/d" : integer.format(value);

/** Input is already a percentage (12.5 -> 12,5). */
export const points = (value: number | null | undefined): string =>
  value === null || value === undefined ? "s/d" : `${decimal.format(value)} %`;

export const dateTimeText = (iso: string | null | undefined): string => {
  if (!iso) return "—";
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? iso : dateTime.format(date);
};

export const durationText = (start: string, end: string | null): string => {
  if (!end) return "en curso";
  const ms = new Date(end).getTime() - new Date(start).getTime();
  if (Number.isNaN(ms) || ms < 0) return "—";
  return ms < 1000 ? `${ms} ms` : `${decimal.format(ms / 1000)} s`;
};
