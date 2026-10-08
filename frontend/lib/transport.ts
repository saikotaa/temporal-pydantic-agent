// A ChatTransport for useChat over agent-api.
//
// `sendMessages` POSTs a `send` command (client_message_id = the UI message id) and returns an
// empty stream: the reply does not come back on the request, it comes back on the conversation's
// SSE stream. `reconnectToStream` opens that stream (`GET /agent/stream?after=<last id>`) and
// converts SSE frames into UIMessageChunks. The AI SDK's resume path resets its state on every
// `start` chunk with a new message id, so each turn becomes its own assistant message, and the
// SSE `id:` doubles as the reconnect cursor.

import type { ChatTransport, UIMessage, UIMessageChunk } from "ai";
import { API_URL, postCommand } from "./api";

export class ConversationTransport implements ChatTransport<UIMessage> {
  lastEventId: string | null = null;
  /** True while a reconnect stream is open; the UI only re-resumes when it is not. */
  streamOpen = false;

  constructor(private readonly onEventId?: (id: string) => void) {}

  async sendMessages(options: {
    trigger: "submit-message" | "regenerate-message";
    chatId: string;
    messageId: string | undefined;
    messages: UIMessage[];
    abortSignal: AbortSignal | undefined;
  }): Promise<ReadableStream<UIMessageChunk>> {
    const last = options.messages[options.messages.length - 1];
    if (options.trigger === "submit-message" && last?.role === "user") {
      const text = last.parts
        .filter((p): p is { type: "text"; text: string } => p.type === "text")
        .map((p) => p.text)
        .join("\n");
      await postCommand(options.chatId, last.id, { kind: "send", content: text });
    }
    return new ReadableStream<UIMessageChunk>({
      start(controller) {
        controller.close();
      },
    });
  }

  async reconnectToStream(options: {
    chatId: string;
    abortSignal?: AbortSignal;
  }): Promise<ReadableStream<UIMessageChunk> | null> {
    const url = new URL(`${API_URL}/agent/stream`);
    url.searchParams.set("conversation_id", options.chatId);
    if (this.lastEventId) url.searchParams.set("after", this.lastEventId);
    const res = await fetch(url, { signal: options.abortSignal });
    if (!res.ok || !res.body) return null;
    this.streamOpen = true;
    return parseSse(
      res.body,
      (id) => {
        this.lastEventId = id;
        this.onEventId?.(id);
      },
      () => {
        this.streamOpen = false;
      },
    );
  }
}

/** SSE frames -> UIMessageChunk. `[DONE]` ends a turn, not the stream; the stream outlives turns. */
export function parseSse(
  body: ReadableStream<Uint8Array>,
  onId: (id: string) => void,
  onClose?: () => void,
): ReadableStream<UIMessageChunk> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  return new ReadableStream<UIMessageChunk>({
    async pull(controller) {
      while (true) {
        const idx = buffer.indexOf("\n\n");
        if (idx >= 0) {
          const frame = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 2);
          const chunk = parseFrame(frame, onId);
          if (chunk) {
            controller.enqueue(chunk);
            return;
          }
          continue;
        }
        let result: ReadableStreamReadResult<Uint8Array>;
        try {
          result = await reader.read();
        } catch (e) {
          onClose?.();
          throw e;
        }
        if (result.done) {
          onClose?.();
          controller.close();
          return;
        }
        const value = result.value;
        buffer += decoder.decode(value, { stream: true });
      }
    },
    cancel() {
      onClose?.();
      void reader.cancel();
    },
  });
}

function parseFrame(frame: string, onId: (id: string) => void): UIMessageChunk | null {
  let data: string | null = null;
  for (const line of frame.split("\n")) {
    if (line.startsWith("id: ")) onId(line.slice(4));
    else if (line.startsWith("data: ")) data = line.slice(6);
  }
  if (data === null || data === "[DONE]") return null;
  return JSON.parse(data) as UIMessageChunk;
}
