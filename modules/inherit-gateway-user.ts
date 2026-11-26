import type { ZuploContext, ZuploRequest } from "@zuplo/runtime";

/** Key the aggregator uses to hand the authenticated user to upstream routes. */
export const GATEWAY_USER_KEY = "mcpAggregatorUser";

/**
 * Inbound policy for internal upstream MCP routes.
 *
 * The aggregator calls upstream routes with `context.invokeRoute`, which does
 * not carry `request.user` across. This copies the user that the gateway's
 * MCP OAuth policy authenticated on the aggregate route, read from the parent
 * context, so the upstream token-exchange policy knows whose credentials to
 * use. `parentContext` is set only for `invokeRoute` calls, so it cannot be
 * forged by a network client.
 */
export default async function inheritGatewayUser(
  request: ZuploRequest,
  context: ZuploContext
) {
  const user = context.parentContext?.custom[GATEWAY_USER_KEY];
  if (!user) {
    return new Response(null, { status: 404 });
  }
  request.user = user;
  return request;
}
