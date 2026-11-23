"use client";

import { useCallback, useEffect, useState } from "react";

import { ApiError, toApiError } from "@/lib/api";

export type AsyncState<T> =
  | { status: "loading" }
  | { status: "ok"; data: T }
  | { status: "error"; error: ApiError };

interface Slot<T> {
  key: string;
  state: AsyncState<T>;
}

/**
 * Loads data for `key` (any string that identifies the request). A new key shows the loading
 * state immediately, without setting state synchronously inside the effect.
 */
export function useApi<T>(key: string, fetcher: () => Promise<T>): {
  state: AsyncState<T>;
  reload: () => void;
} {
  const [slot, setSlot] = useState<Slot<T> | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let active = true;
    fetcher().then(
      (data) => {
        if (active) setSlot({ key: `${key}#${attempt}`, state: { status: "ok", data } });
      },
      (error: unknown) => {
        if (active)
          setSlot({ key: `${key}#${attempt}`, state: { status: "error", error: toApiError(error) } });
      },
    );
    return () => {
      active = false;
    };
    // The fetcher is recreated on each render; `key` and `attempt` identify the request.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, attempt]);

  const reload = useCallback(() => setAttempt((n) => n + 1), []);
  const current = slot && slot.key === `${key}#${attempt}` ? slot.state : { status: "loading" as const };
  return { state: current, reload };
}
