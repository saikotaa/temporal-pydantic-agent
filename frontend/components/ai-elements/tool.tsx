"use client";
import { useState, type ReactNode } from "react";
import { ChevronDown, Wrench } from "lucide-react";

export type ToolState =
  | "input-streaming"
  | "input-available"
  | "approval-requested"
  | "approval-responded"
  | "output-available"
  | "output-error"
  | "output-denied";

export function Tool({ children, defaultOpen = false }: { children: ReactNode; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="rounded-lg border border-neutral-300 text-xs dark:border-neutral-700" data-open={open}>
      <ToolOpenContext.Provider value={{ open, toggle: () => setOpen((o) => !o) }}>{children}</ToolOpenContext.Provider>
    </div>
  );
}

import { createContext, useContext } from "react";
const ToolOpenContext = createContext<{ open: boolean; toggle: () => void }>({ open: false, toggle: () => {} });

const STATE_LABEL: Record<string, string> = {
  "input-streaming": "Pending",
  "input-available": "Running",
  "approval-requested": "Awaiting approval",
  "output-available": "Completed",
  "output-error": "Error",
  "output-denied": "Denied",
};

export function ToolHeader({ type, state }: { type: string; state: string }) {
  const { open, toggle } = useContext(ToolOpenContext);
  return (
    <button type="button" onClick={toggle} className="flex w-full items-center gap-2 px-3 py-2 text-left">
      <Wrench className="size-3.5" />
      <span className="font-mono">{type.replace(/^tool-/, "")}</span>
      <span className="ml-auto rounded bg-neutral-200 px-1.5 py-0.5 dark:bg-neutral-700">{STATE_LABEL[state] ?? state}</span>
      <ChevronDown className={`size-3.5 transition-transform ${open ? "rotate-180" : ""}`} />
    </button>
  );
}

export function ToolContent({ children }: { children: ReactNode }) {
  const { open } = useContext(ToolOpenContext);
  return open ? <div className="space-y-2 border-t border-neutral-200 p-3 dark:border-neutral-700">{children}</div> : null;
}

export function ToolInput({ input }: { input: unknown }) {
  return (
    <div>
      <div className="mb-1 font-medium uppercase text-neutral-500">Parameters</div>
      <pre className="overflow-x-auto rounded bg-neutral-100 p-2 dark:bg-neutral-900">{JSON.stringify(input, null, 2)}</pre>
    </div>
  );
}

export function ToolOutput({ output, errorText }: { output?: unknown; errorText?: string }) {
  if (output === undefined && !errorText) return null;
  return (
    <div>
      <div className="mb-1 font-medium uppercase text-neutral-500">{errorText ? "Error" : "Result"}</div>
      <pre className={`overflow-x-auto rounded p-2 ${errorText ? "bg-red-50 text-red-700 dark:bg-red-950" : "bg-neutral-100 dark:bg-neutral-900"}`}>
        {errorText ?? (typeof output === "string" ? output : JSON.stringify(output, null, 2))}
      </pre>
    </div>
  );
}
