"use client";
import { useState, type FormEvent } from "react";
import { Send, Square } from "lucide-react";

export function PromptInput({
  onSubmit,
  onStop,
  canStop,
  placeholder = "Send a message…",
}: {
  onSubmit: (text: string) => void | Promise<void>;
  onStop?: () => void;
  canStop?: boolean;
  placeholder?: string;
}) {
  const [text, setText] = useState("");
  const submit = (e: FormEvent) => {
    e.preventDefault();
    const value = text.trim();
    if (!value) return;
    setText("");
    void onSubmit(value);
  };
  return (
    <form onSubmit={submit} className="flex gap-2 border-t border-neutral-200 p-3 dark:border-neutral-800">
      <input
        aria-label="prompt"
        className="flex-1 rounded-lg border border-neutral-300 bg-transparent px-3 py-2 text-sm dark:border-neutral-700"
        value={text}
        placeholder={placeholder}
        onChange={(e) => setText(e.target.value)}
      />
      {canStop && onStop && (
        <button type="button" onClick={onStop} aria-label="stop" className="rounded-lg border border-red-300 px-3 text-red-600">
          <Square className="size-4" />
        </button>
      )}
      <button type="submit" aria-label="send" className="rounded-lg bg-neutral-900 px-3 text-white dark:bg-neutral-100 dark:text-neutral-900">
        <Send className="size-4" />
      </button>
    </form>
  );
}
