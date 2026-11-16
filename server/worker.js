import { DurableObject } from "cloudflare:workers";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/") {
      return new Response("TERRITORY WARS multiplayer server is running");
    }
    if (url.pathname !== "/ws") return new Response("Not found", { status: 404 });
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }
    const id = env.GAME_ROOM.idFromName("territory-wars-main");
    return env.GAME_ROOM.get(id).fetch(request);
  }
};

export class GameRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.nextPlayerId = 1;
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server);
    const playerId = String(this.nextPlayerId++);
    server.serializeAttachment({ playerId });
    server.send(JSON.stringify({ type: "welcome", playerId, players: this.ctx.getWebSockets().length }));
    this.broadcast({ type: "players", count: this.ctx.getWebSockets().length }, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(socket, message) {
    if (typeof message !== "string" || message.length > 4096) return;
    let packet;
    try { packet = JSON.parse(message); } catch { return; }
    if (!packet || packet.type !== "state" ||
        !Number.isFinite(packet.x) || !Number.isFinite(packet.y) ||
        !Number.isFinite(packet.angle)) return;
    const attachment = socket.deserializeAttachment() || {};
    this.broadcast({
      type: "player_state",
      playerId: attachment.playerId || "unknown",
      x: packet.x, y: packet.y, angle: packet.angle,
      trail: Array.isArray(packet.trail) ? packet.trail.slice(-120) : []
    }, socket);
  }

  webSocketClose() {
    this.broadcast({ type: "players", count: Math.max(0, this.ctx.getWebSockets().length) });
  }

  webSocketError(socket) {
    try { socket.close(1011, "Connection error"); } catch {}
  }

  broadcast(data, exclude = null) {
    const payload = JSON.stringify(data);
    for (const socket of this.ctx.getWebSockets()) {
      if (socket === exclude) continue;
      try { socket.send(payload); } catch {}
    }
  }
}
