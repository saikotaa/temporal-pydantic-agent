"use client";
// Chat over agent-api: useChat + ConversationTransport (stream), direct commands (queue, stop,
// approvals), and server state polling for the queue panel.

import { useChat } from "@ai-sdk/react";
import { isToolUIPart, getToolName, type UIMessage } from "ai";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Conversation, ConversationContent, ConversationEmptyState } from "@/components/ai-elements/conversation";
import { Message, MessageContent } from "@/components/ai-elements/message";
import { PromptInput } from "@/components/ai-elements/prompt-input";
import { Reasoning } from "@/components/ai-elements/reasoning";
import { Response } from "@/components/ai-elements/response";
import { Tool, ToolContent, ToolHeader, ToolInput, ToolOutput } from "@/components/ai-elements/tool";
import { getState, newId, postCommand, type ConversationState, type PendingApproval } from "@/lib/api";
import { ConversationTransport } from "@/lib/transport";

type ApprovalData = { turnId: string; approvals: PendingApproval[] };

export function Chat({ conversationId }: { conversationId: string }) {
  const [lastEventId, setLastEventId] = useState<string | null>(null);
  const [serverState, setServerState] = useState<ConversationState | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const transport = useMemo(() => new ConversationTransport(setLastEventId), []);
  const refresh = useCallback(async () => {
    try {
      setServerState(await getState(conversationId));
    } catch (e) {
      setNotice(String(e));
    }
  }, [conversationId]);

  const { messages, sendMessage, resumeStream, status } = useChat({
    id: conversationId,
    transport,
    resume: true,
    onData: (part) => {
      if (part.type === "data-status" || part.type === "data-queue") void refresh();
      if (part.type === "data-retry") setNotice(`model call retried (attempt ${(part.data as { attempt: number }).attempt})`);
    },
    onError: (e) => setNotice(e.message),
  });

  // The SSE stream outlives turns; if it drops, reconnect from the last id.
  const resumeRef = useRef(resumeStream);
  useEffect(() => {
    resumeRef.current = resumeStream;
  }, [resumeStream]);
  useEffect(() => {
    if (status === "ready" || status === "error") {
      const t = setTimeout(() => {
        if (!transport.streamOpen) void resumeRef.current();
      }, 1000);
      return () => clearTimeout(t);
    }
  }, [status, lastEventId, transport]);

  useEffect(() => {
    const first = setTimeout(() => void refresh(), 0);
    const t = setInterval(() => void refresh(), 2000);
    return () => {
      clearTimeout(first);
      clearInterval(t);
    };
  }, [refresh]);

  const command = useCallback(
    async (payload: Parameters<typeof postCommand>[2]) => {
      try {
        const ack = await postCommand(conversationId, newId("c"), payload);
        if (!ack.accepted) setNotice(`${payload.kind}: ${ack.detail ?? "rejected"}`);
      } catch (e) {
        setNotice(String(e));
      }
      await refresh();
    },
    [conversationId, refresh],
  );

  const pendingApproval = useMemo(() => latestApproval(messages), [messages]);
  const working = serverState?.status === "working";

  return (
    <div className="flex h-dvh flex-col md:flex-row">
      <main className="flex min-h-0 flex-1 flex-col">
        <header className="flex items-center gap-3 border-b border-neutral-200 px-4 py-2 text-sm dark:border-neutral-800">
          <span className="font-mono">{conversationId}</span>
          <StatusPill status={serverState?.status ?? "…"} />
          <span className="text-neutral-500">turn {serverState?.turn_count ?? 0} · run {serverState?.run_number ?? 0}</span>
          <span className="ml-auto text-xs text-neutral-400">stream {status} · last id {lastEventId ?? "–"}</span>
        </header>
        {notice && (
          <div className="flex items-center justify-between bg-amber-50 px-4 py-1 text-xs text-amber-800 dark:bg-amber-950 dark:text-amber-200">
            {notice}
            <button type="button" onClick={() => setNotice(null)}>dismiss</button>
          </div>
        )}
        <Conversation>
          <ConversationContent>
            {messages.length === 0 && (
              <ConversationEmptyState title="No messages yet" description="Try: what is the weather in Oslo? · read the runbook note · open a ticket about the login bug" />
            )}
            {messages.filter(hasVisibleParts).map((m) => (
              <MessageView key={m.id} message={m} />
            ))}
            {pendingApproval && serverState?.status === "blocked" && (
              <ApprovalCard approval={pendingApproval} onDecide={(results) => command({ kind: "tool_results", results })} />
            )}
          </ConversationContent>
        </Conversation>
        <PromptInput
          onSubmit={(text) => sendMessage({ text })}
          canStop={working}
          onStop={() => command({ kind: "stop", expected_turn_id: serverState?.current_turn_id ?? null })}
          placeholder={working ? "Queue another message, or steer the running turn below…" : "Send a message…"}
        />
        <SteerBar disabled={!working} onSteer={(content) => command({ kind: "steer", content })} />
      </main>
      <QueuePanel state={serverState} onCommand={command} />
    </div>
  );
}

function StatusPill({ status }: { status: string }) {
  const color =
    status === "working" ? "bg-blue-100 text-blue-800" : status === "blocked" ? "bg-amber-100 text-amber-800" : "bg-neutral-100 text-neutral-700";
  return <span data-testid="status" className={`rounded-full px-2 py-0.5 text-xs ${color}`}>{status}</span>;
}

function MessageView({ message }: { message: UIMessage }) {
  return (
    <Message from={message.role}>
      <MessageContent>
        {message.parts.map((part, i) => {
          if (part.type === "text") return message.role === "assistant" ? <Response key={i}>{part.text}</Response> : <span key={i}>{part.text}</span>;
          if (part.type === "reasoning") return <Reasoning key={i} isStreaming={part.state === "streaming"}>{part.text}</Reasoning>;
          if (isToolUIPart(part)) {
            return (
              <Tool key={part.toolCallId} defaultOpen={part.state === "output-error"}>
                <ToolHeader type={`tool-${getToolName(part)}`} state={part.state} />
                <ToolContent>
                  <ToolInput input={part.input} />
                  <ToolOutput output={"output" in part ? part.output : undefined} errorText={"errorText" in part ? part.errorText : undefined} />
                </ToolContent>
              </Tool>
            );
          }
          if (part.type === "data-request-resolved") {
            const d = part.data as { approvals: { tool_call_id: string; approved: boolean }[] };
            return (
              <div key={i} className="text-xs text-neutral-500">
                {d.approvals.map((a) => `${a.approved ? "approved" : "declined"} ${a.tool_call_id}`).join(", ")}
              </div>
            );
          }
          if (part.type === "step-start") return <hr key={i} className="border-neutral-200 dark:border-neutral-700" />;
          return null;
        })}
      </MessageContent>
    </Message>
  );
}

/** A chunk arriving before a turn's `start` creates an empty assistant message; hide those. */
function hasVisibleParts(m: UIMessage): boolean {
  return m.parts.some((p) => p.type !== "step-start");
}

function latestApproval(messages: UIMessage[]): ApprovalData | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    for (const part of messages[i].parts) {
      if (part.type === "data-approval") return part.data as ApprovalData;
    }
  }
  return null;
}

function ApprovalCard({
  approval,
  onDecide,
}: {
  approval: ApprovalData;
  onDecide: (results: { tool_call_id: string; decision: "accept" | "decline"; reason?: string }[]) => void;
}) {
  return (
    <div data-testid="approval-card" className="rounded-xl border border-amber-300 bg-amber-50 p-4 text-sm dark:bg-amber-950">
      <div className="mb-2 font-medium">Approval required ({approval.turnId})</div>
      {approval.approvals.map((a) => (
        <div key={a.tool_call_id} className="mb-2">
          <div className="font-mono text-xs">{a.tool_name}</div>
          <pre className="rounded bg-white/60 p-2 text-xs dark:bg-black/30">{JSON.stringify(a.args, null, 2)}</pre>
          <div className="mt-2 flex gap-2">
            <button type="button" className="rounded bg-green-600 px-3 py-1 text-white" onClick={() => onDecide([{ tool_call_id: a.tool_call_id, decision: "accept" }])}>
              Approve
            </button>
            <button type="button" className="rounded bg-red-600 px-3 py-1 text-white" onClick={() => onDecide([{ tool_call_id: a.tool_call_id, decision: "decline", reason: "declined in UI" }])}>
              Decline
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}

function SteerBar({ disabled, onSteer }: { disabled: boolean; onSteer: (content: string) => void }) {
  const [text, setText] = useState("");
  return (
    <form
      className="flex gap-2 px-3 pb-3 text-xs"
      onSubmit={(e) => {
        e.preventDefault();
        if (!text.trim()) return;
        onSteer(text.trim());
        setText("");
      }}
    >
      <input
        aria-label="steer"
        disabled={disabled}
        value={text}
        onChange={(e) => setText(e.target.value)}
        placeholder={disabled ? "Steer (only while a turn is running)" : "Steer the running turn…"}
        className="flex-1 rounded border border-neutral-300 bg-transparent px-2 py-1 disabled:opacity-50 dark:border-neutral-700"
      />
      <button type="submit" disabled={disabled} className="rounded border px-2 disabled:opacity-50">steer</button>
    </form>
  );
}

function QueuePanel({
  state,
  onCommand,
}: {
  state: ConversationState | null;
  onCommand: (payload: Parameters<typeof postCommand>[2]) => Promise<void>;
}) {
  const [editing, setEditing] = useState<{ id: string; content: string } | null>(null);
  const pending = state?.pending ?? [];
  const move = (index: number, delta: number) => {
    const order = pending.map((m) => m.client_message_id);
    const j = index + delta;
    if (j < 0 || j >= order.length) return;
    [order[index], order[j]] = [order[j], order[index]];
    void onCommand({ kind: "reorder", order });
  };
  return (
    <aside data-testid="queue" className="w-full border-t border-neutral-200 p-3 text-sm md:w-80 md:border-l md:border-t-0 dark:border-neutral-800">
      <div className="mb-2 flex items-center justify-between">
        <span className="font-medium">Queue</span>
        <span className="text-xs text-neutral-500">{pending.length} pending</span>
      </div>
      {state?.current_client_message_id && (
        <div className="mb-2 rounded border border-blue-200 bg-blue-50 p-2 text-xs dark:bg-blue-950">
          running: <span className="font-mono">{state.current_turn_id}</span> ({state.current_client_message_id})
        </div>
      )}
      {pending.length === 0 && <div className="text-xs text-neutral-500">Nothing queued. Send while a turn runs to queue.</div>}
      <ol className="space-y-2">
        {pending.map((m, i) => (
          <li key={m.client_message_id} className="rounded border border-neutral-200 p-2 text-xs dark:border-neutral-700">
            {editing?.id === m.client_message_id ? (
              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  void onCommand({ kind: "edit", target_client_message_id: m.client_message_id, content: editing.content });
                  setEditing(null);
                }}
              >
                <input className="w-full rounded border px-1" value={editing.content} onChange={(e) => setEditing({ id: m.client_message_id, content: e.target.value })} autoFocus />
                <div className="mt-1 flex gap-1">
                  <button type="submit" className="rounded border px-1">save</button>
                  <button type="button" className="rounded border px-1" onClick={() => setEditing(null)}>cancel</button>
                </div>
              </form>
            ) : (
              <>
                <div className="mb-1 flex items-center gap-1 text-neutral-500">
                  <span className="rounded bg-neutral-100 px-1 dark:bg-neutral-800">{m.kind}</span>
                  <span className="font-mono">{m.client_message_id.slice(0, 12)}</span>
                </div>
                <div className="whitespace-pre-wrap">{m.content || (m.kind === "tool_results" ? "(tool results)" : "")}</div>
                <div className="mt-1 flex flex-wrap gap-1">
                  <button type="button" className="rounded border px-1" onClick={() => setEditing({ id: m.client_message_id, content: m.content })}>edit</button>
                  <button type="button" className="rounded border px-1" onClick={() => onCommand({ kind: "delete", target_client_message_id: m.client_message_id })}>delete</button>
                  <button type="button" className="rounded border px-1" onClick={() => move(i, -1)}>↑</button>
                  <button type="button" className="rounded border px-1" onClick={() => move(i, 1)}>↓</button>
                  <button type="button" className="rounded border px-1" onClick={() => onCommand({ kind: "send_now", target_client_message_id: m.client_message_id })}>send now</button>
                </div>
              </>
            )}
          </li>
        ))}
      </ol>
      {state?.pending_approvals.length ? (
        <div className="mt-3 text-xs text-amber-700">{state.pending_approvals.length} approval(s) pending</div>
      ) : null}
    </aside>
  );
}
