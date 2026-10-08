"use client";
import { Streamdown } from "streamdown";

export function Response({ children }: { children: string }) {
  return (
    <div className="prose prose-sm max-w-none dark:prose-invert">
      <Streamdown>{children}</Streamdown>
    </div>
  );
}
