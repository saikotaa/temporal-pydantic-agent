"use client";
import { useState } from "react";
import { Brain } from "lucide-react";

export function Reasoning({ children, isStreaming }: { children: string; isStreaming?: boolean }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="text-xs text-neutral-500">
      <button type="button" onClick={() => setOpen((o) => !o)} className="flex items-center gap-1">
        <Brain className="size-3.5" /> {isStreaming ? "Thinking…" : "Thought"}
      </button>
      {open && <div className="mt-1 whitespace-pre-wrap border-l-2 border-neutral-300 pl-2">{children}</div>}
    </div>
  );
}
