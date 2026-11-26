import type { ZuploContext, ZuploRequest } from "@zuplo/runtime";
import { GATEWAY_USER_KEY } from "./inherit-gateway-user";

/**
 * MCP Aggregator
 *
 * Exposes several upstream MCP servers behind a single MCP endpoint. Each
 * upstream is still configured as its own gateway route (McpProxyHandler plus
 * its token-exchange / capability-filter policies). This handler talks to
 * those routes with `context.invokeRoute`, so every per-upstream policy keeps
 * running exactly as it would for a direct connection.
 *
 * Tools are namespaced as `<upstream>__<tool>` so names never collide.
 */

interface UpstreamConfig {
  /** Prefix used to namespace this upstream's tools, e.g. "linear". */
  name: string;
  /** Gateway route that proxies this upstream, e.g. "/mcp/linear". */
  route: string;
}

interface AggregatorOptions {
  upstreams: UpstreamConfig[];
  serverName?: string;
  serverVersion?: string;
  /** Separator between upstream name and tool name. Defaults to "__". */
  separator?: string;
}

type JsonRpcId = string | number;

interface JsonRpcMessage {
  jsonrpc: "2.0";
  id?: JsonRpcId | null;
  method?: string;
  params?: Record<string, unknown>;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

const SUPPORTED_PROTOCOL_VERSIONS = ["2025-06-18", "2025-03-26", "2024-11-05"];
const LATEST_PROTOCOL_VERSION = SUPPORTED_PROTOCOL_VERSIONS[0];

// Upstream session ids, keyed by `${clientSessionId}|${upstreamName}`.
// In-memory and per-isolate: on a miss we simply re-initialize the upstream.
const MAX_CACHED_SESSIONS = 5000;
const upstreamSessions = new Map<string, string>();

function rememberSession(key: string, sessionId: string) {
  if (upstreamSessions.size >= MAX_CACHED_SESSIONS) {
    const oldest = upstreamSessions.keys().next().value;
    if (oldest !== undefined) upstreamSessions.delete(oldest);
  }
  upstreamSessions.set(key, sessionId);
}

class UpstreamError extends Error {
  constructor(
    message: string,
    readonly rpcError?: JsonRpcMessage["error"],
    readonly status?: number
  ) {
    super(message);
  }
}

export default async function mcpAggregator(
  request: ZuploRequest,
  context: ZuploContext
): Promise<Response> {
  const options = (context.route.handler.options ?? {}) as AggregatorOptions;
  if (!options.upstreams?.length) {
    return new Response("MCP aggregator has no upstreams configured", {
      status: 500,
    });
  }
  // Hand the authenticated user to upstream routes (see inherit-gateway-user).
  context.custom[GATEWAY_USER_KEY] = request.user;
  const aggregator = new Aggregator(request, context, options);

  if (request.method === "GET") {
    // No server-initiated SSE stream; allowed by the Streamable HTTP spec.
    return new Response(null, { status: 405, headers: { Allow: "POST, DELETE" } });
  }
  if (request.method === "DELETE") {
    aggregator.forgetSessions();
    return new Response(null, { status: 204 });
  }
  if (request.method !== "POST") {
    return new Response(null, { status: 405, headers: { Allow: "POST, DELETE" } });
  }

  let body: JsonRpcMessage | JsonRpcMessage[];
  try {
    body = await request.json();
  } catch {
    return jsonResponse(rpcError(null, -32700, "Parse error"), 400);
  }

  const messages = Array.isArray(body) ? body : [body];
  const responses: JsonRpcMessage[] = [];
  let newSessionId: string | undefined;

  for (const message of messages) {
    if (message.method === "initialize" && message.id !== undefined) {
      newSessionId = crypto.randomUUID();
    }
    const response = await aggregator.handle(message);
    if (response) responses.push(response);
  }

  const headers: Record<string, string> = {};
  if (newSessionId) headers["mcp-session-id"] = newSessionId;

  if (responses.length === 0) {
    return new Response(null, { status: 202, headers });
  }
  return jsonResponse(Array.isArray(body) ? responses : responses[0], 200, headers);
}

class Aggregator {
  private readonly separator: string;
  private readonly clientSessionId: string | null;
  private readonly protocolVersion: string;

  constructor(
    private readonly request: ZuploRequest,
    private readonly context: ZuploContext,
    private readonly options: AggregatorOptions
  ) {
    this.separator = options.separator ?? "__";
    this.clientSessionId = request.headers.get("mcp-session-id");
    this.protocolVersion =
      request.headers.get("mcp-protocol-version") ?? LATEST_PROTOCOL_VERSION;
  }

  async handle(message: JsonRpcMessage): Promise<JsonRpcMessage | undefined> {
    // Notifications and client responses need no reply.
    if (message.id === undefined || message.id === null || !message.method) {
      return undefined;
    }
    const id = message.id;

    try {
      switch (message.method) {
        case "initialize":
          return rpcResult(id, this.initialize(message.params));
        case "ping":
          return rpcResult(id, {});
        case "tools/list":
          return rpcResult(id, await this.listTools());
        case "tools/call":
          return await this.callTool(id, message.params ?? {});
        default:
          return rpcError(id, -32601, `Method not found: ${message.method}`);
      }
    } catch (err) {
      if (err instanceof UpstreamError && err.rpcError) {
        return { jsonrpc: "2.0", id, error: err.rpcError };
      }
      this.context.log.error("MCP aggregator error", err);
      return rpcError(id, -32603, err instanceof Error ? err.message : "Internal error");
    }
  }

  forgetSessions() {
    if (!this.clientSessionId) return;
    for (const upstream of this.options.upstreams) {
      upstreamSessions.delete(`${this.clientSessionId}|${upstream.name}`);
    }
  }

  private initialize(params: Record<string, unknown> | undefined) {
    const requested = params?.protocolVersion as string | undefined;
    const protocolVersion =
      requested && SUPPORTED_PROTOCOL_VERSIONS.includes(requested)
        ? requested
        : LATEST_PROTOCOL_VERSION;
    return {
      protocolVersion,
      capabilities: { tools: { listChanged: false } },
      serverInfo: {
        name: this.options.serverName ?? "mcp-aggregator",
        version: this.options.serverVersion ?? "1.0.0",
      },
      instructions: `Tools are namespaced as <server>${this.separator}<tool>. Servers: ${this.options.upstreams
        .map((u) => u.name)
        .join(", ")}.`,
    };
  }

  private async listTools() {
    const results = await Promise.allSettled(
      this.options.upstreams.map(async (upstream) => {
        const tools: Array<Record<string, unknown>> = [];
        let cursor: string | undefined;
        do {
          const result = (await this.send(upstream, "tools/list", cursor ? { cursor } : {})) as {
            tools?: Array<Record<string, unknown>>;
            nextCursor?: string;
          };
          tools.push(...(result.tools ?? []));
          cursor = result.nextCursor;
        } while (cursor);

        return tools.map((tool) => ({
          ...tool,
          name: `${upstream.name}${this.separator}${tool.name}`,
          description: `[${upstream.name}] ${tool.description ?? ""}`.trim(),
        }));
      })
    );

    const tools: Array<Record<string, unknown>> = [];
    results.forEach((result, i) => {
      if (result.status === "fulfilled") {
        tools.push(...result.value);
      } else if (connectRequiredOf(result.reason)) {
        // The user hasn't authorized this upstream yet. Expose a tool that
        // hands back the gateway's connect link instead of silently hiding it.
        const upstream = this.options.upstreams[i];
        tools.push({
          name: `${upstream.name}${this.separator}${CONNECT_TOOL}`,
          description: `${upstream.name} is not connected for this user yet. Call this tool to get a link that authorizes access to ${upstream.name}, then reconnect to load its tools.`,
          inputSchema: { type: "object", properties: {} },
        });
      } else {
        // One unhealthy upstream should not hide the rest.
        this.context.log.warn(
          `MCP aggregator: tools/list failed for upstream "${this.options.upstreams[i].name}"`,
          result.reason instanceof Error ? result.reason.message : result.reason
        );
      }
    });
    return { tools };
  }

  private async callTool(id: JsonRpcId, params: Record<string, unknown>) {
    const fullName = String(params.name ?? "");
    const splitAt = fullName.indexOf(this.separator);
    const upstream =
      splitAt > 0
        ? this.options.upstreams.find((u) => u.name === fullName.slice(0, splitAt))
        : undefined;
    if (!upstream) {
      return rpcError(id, -32602, `Unknown tool: ${fullName}`);
    }
    const toolName = fullName.slice(splitAt + this.separator.length);
    try {
      if (toolName === CONNECT_TOOL) {
        await this.send(upstream, "tools/list", {});
        return rpcResult(id, textResult(
          `${upstream.name} is already connected. Reconnect to this MCP server (or refresh its tools) to load the ${upstream.name} tools.`
        ));
      }
      const result = await this.send(upstream, "tools/call", { ...params, name: toolName });
      return rpcResult(id, result);
    } catch (err) {
      const connect = connectRequiredOf(err);
      if (!connect) throw err;
      return rpcResult(id, textResult(
        `${connect.message ?? `Connect ${upstream.name} to continue.`}\n\nOpen this link in a browser to authorize access:\n${connect.authUrl}\n\nAfter authorizing, reconnect to this MCP server to load the ${upstream.name} tools.`,
        true
      ));
    }
  }

  /** Send one request to an upstream route, initializing a session if needed. */
  private async send(
    upstream: UpstreamConfig,
    method: string,
    params: Record<string, unknown>,
    retried = false
  ): Promise<unknown> {
    const sessionId = await this.ensureSession(upstream);
    const response = await this.post(upstream, sessionId, {
      jsonrpc: "2.0",
      id: crypto.randomUUID(),
      method,
      params,
    });

    // Upstream session expired: re-initialize once.
    if (response.status === 404 && sessionId && !retried) {
      this.dropSession(upstream);
      return this.send(upstream, method, params, true);
    }
    const message = await readRpcResponse(response, upstream.name);
    if (message.error) {
      throw new UpstreamError(message.error.message, message.error, response.status);
    }
    return message.result;
  }

  private async ensureSession(upstream: UpstreamConfig): Promise<string | undefined> {
    const key = this.sessionKey(upstream);
    const cached = key ? upstreamSessions.get(key) : undefined;
    if (cached) return cached;

    const response = await this.post(upstream, undefined, {
      jsonrpc: "2.0",
      id: crypto.randomUUID(),
      method: "initialize",
      params: {
        protocolVersion: this.protocolVersion,
        capabilities: {},
        clientInfo: { name: this.options.serverName ?? "mcp-aggregator", version: "1.0.0" },
      },
    });
    const message = await readRpcResponse(response, upstream.name);
    if (message.error) {
      throw new UpstreamError(message.error.message, message.error, response.status);
    }

    const sessionId = response.headers.get("mcp-session-id") ?? undefined;
    await this.post(upstream, sessionId, {
      jsonrpc: "2.0",
      method: "notifications/initialized",
    });
    if (key && sessionId) rememberSession(key, sessionId);
    return sessionId;
  }

  private dropSession(upstream: UpstreamConfig) {
    const key = this.sessionKey(upstream);
    if (key) upstreamSessions.delete(key);
  }

  private sessionKey(upstream: UpstreamConfig) {
    return this.clientSessionId ? `${this.clientSessionId}|${upstream.name}` : undefined;
  }

  private post(
    upstream: UpstreamConfig,
    sessionId: string | undefined,
    message: JsonRpcMessage
  ): Promise<Response> {
    const headers = new Headers({
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "mcp-protocol-version": this.protocolVersion,
    });
    if (sessionId) headers.set("mcp-session-id", sessionId);

    // Upstream routes are internal-only and pick up the authenticated user
    // from this context (see inherit-gateway-user).
    // Use an absolute URL on the caller's origin so URLs the gateway builds
    // from the request (e.g. the upstream connect link) use the public host.
    return this.context.invokeRoute(new URL(upstream.route, this.request.url), {
      method: "POST",
      headers,
      body: JSON.stringify(message),
    });
  }
}

/** Read a single JSON-RPC response from a JSON or SSE upstream response. */
async function readRpcResponse(response: Response, upstreamName: string): Promise<JsonRpcMessage> {
  const contentType = response.headers.get("content-type") ?? "";
  const text = await response.text();

  if (!response.ok && !text) {
    throw new UpstreamError(`Upstream "${upstreamName}" returned HTTP ${response.status}`);
  }

  let candidates: unknown[] = [];
  try {
    if (contentType.includes("text/event-stream")) {
      candidates = text
        .split(/\r?\n\r?\n/)
        .map((event) =>
          event
            .split(/\r?\n/)
            .filter((line) => line.startsWith("data:"))
            .map((line) => line.slice(5).trimStart())
            .join("\n")
        )
        .filter(Boolean)
        .map((data) => JSON.parse(data));
    } else if (text) {
      const parsed = JSON.parse(text);
      candidates = Array.isArray(parsed) ? parsed : [parsed];
    }
  } catch {
    throw new UpstreamError(
      `Upstream "${upstreamName}" returned an unreadable response (HTTP ${response.status})`
    );
  }

  const message = candidates.find(
    (c): c is JsonRpcMessage =>
      typeof c === "object" && c !== null && ("result" in c || "error" in c)
  );
  if (!message) {
    throw new UpstreamError(
      `Upstream "${upstreamName}" returned no JSON-RPC response (HTTP ${response.status})`
    );
  }
  return message;
}

const CONNECT_TOOL = "connect_account";

interface ConnectRequired {
  authUrl?: string;
  message?: string;
}

/** Extract the gateway's connect-required payload from an upstream error. */
function connectRequiredOf(err: unknown): ConnectRequired | undefined {
  if (!(err instanceof UpstreamError)) return undefined;
  const data = err.rpcError?.data as
    | { connectRequired?: ConnectRequired; elicitations?: Array<{ url?: string; message?: string }> }
    | undefined;
  if (data?.connectRequired) return data.connectRequired;
  const elicitation = data?.elicitations?.find((e) => e.url);
  return elicitation ? { authUrl: elicitation.url, message: elicitation.message } : undefined;
}

function textResult(text: string, isError = false) {
  return { content: [{ type: "text", text }], isError };
}

function rpcResult(id: JsonRpcId, result: unknown): JsonRpcMessage {
  return { jsonrpc: "2.0", id, result };
}

function rpcError(id: JsonRpcId | null, code: number, message: string): JsonRpcMessage {
  return { jsonrpc: "2.0", id, error: { code, message } };
}

function jsonResponse(body: unknown, status = 200, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}
