"use client";

import { useSyncExternalStore } from "react";

export const DEFAULT_WORKSPACE = "default";
const STORAGE_KEY = "dataops.workspace";
const CHANGE_EVENT = "dataops:workspace";
/** Same rule as the API (src/engine/workspace.py). */
export const WORKSPACE_PATTERN = /^[a-z0-9][a-z0-9_-]{0,47}$/;

export function isValidWorkspace(value: string): boolean {
  return WORKSPACE_PATTERN.test(value);
}

function subscribe(onChange: () => void): () => void {
  window.addEventListener("storage", onChange);
  window.addEventListener(CHANGE_EVENT, onChange);
  return () => {
    window.removeEventListener("storage", onChange);
    window.removeEventListener(CHANGE_EVENT, onChange);
  };
}

function readStored(): string {
  const stored = window.localStorage.getItem(STORAGE_KEY);
  return stored && isValidWorkspace(stored) ? stored : DEFAULT_WORKSPACE;
}

/** Active workspace. Server render and first client render both use the default value. */
export function useWorkspace(): string {
  return useSyncExternalStore(subscribe, readStored, () => DEFAULT_WORKSPACE);
}

export function setWorkspace(value: string): void {
  window.localStorage.setItem(STORAGE_KEY, value);
  window.dispatchEvent(new Event(CHANGE_EVENT));
}
