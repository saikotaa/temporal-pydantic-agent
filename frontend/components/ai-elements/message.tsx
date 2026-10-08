"use client";
import type { ReactNode } from "react";

export function Message({ from, children }: { from: "user" | "assistant" | "system"; children: ReactNode }) {
  const mine = from === "user";
  return (
    <div className={`flex w-full ${mine ? "justify-end" : "justify-start"}`} data-role={from}>
      <div
        className={`max-w-[80%] rounded-2xl px-4 py-2 text-sm ${
          mine ? "bg-neutral-900 text-white dark:bg-neutral-100 dark:text-neutral-900" : "bg-neutral-100 dark:bg-neutral-800"
        }`}
      >
        {children}
      </div>
    </div>
  );
}

export function MessageContent({ children }: { children: ReactNode }) {
  return <div className="flex flex-col gap-2">{children}</div>;
}
