// Thin client for agent-api (toy/api/app.py). Mirrors toy/commands.py and toy/state.py.

export const API_URL =
  process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8400";

export type ConversationStatus = "idle" | "working" | "blocked";
export type TurnTerminal = "completed" | "blocked" | "stopped" | "failed";

export interface QueuedMessage {
  client_message_id: string;
  kind: "send" | "steer" | "context" | "tool_results";
  content: string;
}

export interface PendingApproval {
  tool_call_id: string;
  tool_name: string;
  args: Record<string, unknown>;
}

export interface ConversationState {
  status: ConversationStatus;
  pending: QueuedMessage[];
  current_turn_id: string | null;
  current_client_message_id: string | null;
  turn_count: number;
  pending_approvals: PendingApproval[];
  last_terminal: TurnTerminal | null;
  run_number: number;
}

export type CommandPayload =
  | { kind: "send"; content: string }
  | { kind: "steer"; content: string }
  | { kind: "context"; content: string }
  | { kind: "edit"; target_client_message_id: string; content: string }
  | { kind: "delete"; target_client_message_id: string }
  | { kind: "reorder"; order: string[] }
  | { kind: "send_now"; target_client_message_id: string }
  | { kind: "stop"; expected_turn_id?: string | null }
  | {
      kind: "tool_results";
      results: {
        tool_call_id: string;
        decision: "accept" | "decline";
        reason?: string | null;
      }[];
    };

export interface CommandAck {
  accepted: boolean;
  status: ConversationStatus;
  duplicate: boolean;
  detail: string | null;
  workflow_id: string | null;
}

export async function postCommand(
  conversationId: string,
  clientMessageId: string,
  payload: CommandPayload,
): Promise<CommandAck> {
  const res = await fetch(`${API_URL}/agent/commands`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      conversation_id: conversationId,
      client_message_id: clientMessageId,
      payload,
    }),
  });
  if (!res.ok) throw new Error(`command failed: ${res.status} ${await res.text()}`);
  return (await res.json()) as CommandAck;
}

export async function getState(
  conversationId: string,
): Promise<ConversationState | null> {
  const res = await fetch(`${API_URL}/agent/conversations/${conversationId}/state`);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`state failed: ${res.status}`);
  return (await res.json()) as ConversationState;
}

export function newId(prefix = "m"): string {
  return `${prefix}-${crypto.randomUUID()}`;
}
