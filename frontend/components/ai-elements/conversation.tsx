"use client";
// Minimal stand-ins for AI Elements' Conversation components (same names/props shape).
// Replace with `npx ai-elements@latest add conversation` where the registry is reachable.
import { useEffect, useRef, type ReactNode } from "react";

export function Conversation({ children }: { children: ReactNode }) {
  return <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden">{children}</div>;
}

export function ConversationContent({ children }: { children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    ref.current?.scrollTo({ top: ref.current.scrollHeight });
  });
  return (
    <div ref={ref} className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
      {children}
    </div>
  );
}

export function ConversationEmptyState({ title, description }: { title: string; description?: string }) {
  return (
    <div className="m-auto text-center text-sm text-neutral-500">
      <div className="font-medium text-neutral-700 dark:text-neutral-300">{title}</div>
      {description && <div>{description}</div>}
    </div>
  );
}
