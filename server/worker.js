import { DurableObject } from "cloudflare:workers";

const MAX_PACKET_BYTES = 128 * 1024;
const MAX_NAME_LENGTH = 18;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/") {
      return new Response("TERRITORY WARS shared-room server is running");
    }
    if (url.pathname !== "/ws") return new Response("Not found", { status: 404 });
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }
    const roomId = env.GAME_ROOM.idFromName("territory-wars-main");
    return env.GAME_ROOM.get(roomId).fetch(request);
  }
};

export class GameRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.latestSnapshot = null;
    this.nextId = 1;
  }

  sockets() {
    return this.ctx.getWebSockets();
  }

  hostSocket() {
    return this.sockets().find(socket => {
      const data = socket.deserializeAttachment() || {};
      return data.isHost === true;
    }) || null;
  }

  hostId() {
    const socket = this.hostSocket();
    return socket ? (socket.deserializeAttachment() || {}).playerId : null;
  }

  roster() {
    return this.sockets().map(socket => {
      const data = socket.deserializeAttachment() || {};
      return {
        playerId: data.playerId,
        name: data.name || "Игрок",
        isHost: data.isHost === true
      };
    });
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    const wasEmpty = this.sockets().length === 0;
    const playerId = String(this.nextId++);
    const url = new URL(request.url);
    const requestedName = (url.searchParams.get("name") || "Игрок").trim();
    const name = Array.from(requestedName).slice(0, MAX_NAME_LENGTH).join("") || "Игрок";
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ playerId, name, isHost: wasEmpty });

    server.send(JSON.stringify({
      type: "welcome",
      playerId,
      hostId: wasEmpty ? playerId : this.hostId(),
      players: this.roster(),
      snapshot: wasEmpty ? null : this.latestSnapshot
    }));
    this.broadcast({
      type: "player_joined",
      player: { playerId, name, isHost: wasEmpty },
      players: this.roster()
    }, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(socket, message) {
    if (typeof message !== "string" || message.length > MAX_PACKET_BYTES) return;
    let packet;
    try { packet = JSON.parse(message); } catch { return; }
    if (!packet || typeof packet.type !== "string") return;

    const sender = socket.deserializeAttachment() || {};
    if (packet.type === "input") {
      if (!Number.isFinite(packet.angle)) return;
      const host = this.hostSocket();
      if (host && host !== socket) {
        try {
          host.send(JSON.stringify({
            type: "player_input",
            playerId: sender.playerId,
            angle: Math.max(-Math.PI, Math.min(Math.PI, packet.angle))
          }));
        } catch {}
      }
      return;
    }

    if (packet.type === "snapshot") {
      if (sender.isHost !== true || !packet.world || typeof packet.world !== "object") return;
      const encoded = JSON.stringify(packet.world);
      if (encoded.length > MAX_PACKET_BYTES) return;
      this.latestSnapshot = packet.world;
      this.broadcast({
        type: "world_snapshot",
        hostId: sender.playerId,
        world: packet.world
      }, socket);
      return;
    }

    if (packet.type === "request_snapshot") {
      if (this.latestSnapshot) {
        socket.send(JSON.stringify({
          type: "world_snapshot",
          hostId: this.hostId(),
          world: this.latestSnapshot
        }));
      }
    }
  }

  webSocketClose(socket) {
    this.removeAndAnnounce(socket);
  }

  webSocketError(socket) {
    this.removeAndAnnounce(socket);
    try { socket.close(1011, "Connection error"); } catch {}
  }

  removeAndAnnounce(socket) {
    const data = socket.deserializeAttachment() || {};
    const wasHost = data.isHost === true;
    const remaining = this.sockets().filter(item => item !== socket);
    if (wasHost && remaining.length) {
      const nextHost = remaining[0];
      const nextData = nextHost.deserializeAttachment() || {};
      nextHost.serializeAttachment({ ...nextData, isHost: true });
    }
    const newHostId = this.hostId();
    this.broadcast({
      type: "player_left",
      playerId: data.playerId || null,
      players: this.roster(),
      hostId: newHostId,
      hostChanged: wasHost && !!newHostId
    });
    if (wasHost) this.latestSnapshot = null;
  }

  broadcast(data, exclude = null) {
    const payload = JSON.stringify(data);
    for (const socket of this.sockets()) {
      if (socket === exclude) continue;
      try { socket.send(payload); } catch {}
    }
  }
}
