import { DurableObject } from "cloudflare:workers";

const MAX_PACKET_BYTES = 128 * 1024;
const MAX_NAME_LENGTH = 18;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/") {
      return new Response("TERRITORY WARS shared-room server is running");
    }
    if (url.pathname !== "/ws" && url.pathname !== "/leaderboard") return new Response("Not found", { status: 404 });
    if (url.pathname === "/ws" && request.headers.get("Upgrade") !== "websocket") {
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
        avatar: data.avatar || "",
        uid: data.uid || data.playerId,
        isHost: data.isHost === true,
        slot: data.slot
      };
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/leaderboard") {
      const period = ["all","month","week"].includes(url.searchParams.get("period")) ? url.searchParams.get("period") : "all";
      const events = await this.ctx.storage.get("leaderboardEvents") || [];
      const now = Date.now(), cutoff = period==="week" ? now-7*86400000 : period==="month" ? now-30*86400000 : 0;
      const totals = new Map();
      for (const event of events) {
        if (event.at < cutoff) continue;
        const row = totals.get(event.uid) || {uid:event.uid,name:event.name,avatar:event.avatar||"",games:0,points:0,best:0};
        row.name=event.name||row.name; row.avatar=event.avatar||row.avatar; row.games++; if(event.won)row.points++; row.best=Math.max(row.best,event.score||0); totals.set(event.uid,row);
      }
      return new Response(JSON.stringify({period,players:[...totals.values()].sort((a,b)=>b.points-a.points).slice(0,100)}),{headers:{"Content-Type":"application/json; charset=utf-8","Access-Control-Allow-Origin":"*","Cache-Control":"no-store"}});
    }
    if (request.method === "OPTIONS") return new Response(null,{headers:{"Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"GET, OPTIONS"}});
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    const wasEmpty = this.sockets().length === 0;
    const occupied = new Set(this.roster().map(player => player.slot));
    let slot = 1;
    while (occupied.has(slot) && slot <= 4) slot++;
    if (slot > 4) {
      server.close(4001, "Room is full");
      return new Response("Room is full", { status: 503 });
    }
    const playerId = crypto.randomUUID();
    const requestedName = (url.searchParams.get("name") || "Игрок").trim();
    const name = Array.from(requestedName).slice(0, MAX_NAME_LENGTH).join("") || "Игрок";
    const uid = (url.searchParams.get("uid") || playerId).slice(0,64);
    const rawAvatar = url.searchParams.get("avatar") || "";
    let avatar = "";
    try { const parsed = new URL(rawAvatar); if (parsed.protocol === "https:") avatar = parsed.href.slice(0,1000); } catch {}
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({playerId,name,uid,avatar,isHost:wasEmpty,slot});

    server.send(JSON.stringify({
      type: "welcome",
      playerId,
      hostId: wasEmpty ? playerId : this.hostId(),
      players: this.roster(),
      slot,
      snapshot: wasEmpty ? null : this.latestSnapshot
    }));
    this.broadcast({
      type: "player_joined",
      player: { playerId, name, avatar, isHost: wasEmpty, slot },
      players: this.roster()
    }, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(socket, message) {
    if (typeof message !== "string" || message.length > MAX_PACKET_BYTES) return;
    let packet;
    try { packet = JSON.parse(message); } catch { return; }
    if (!packet || typeof packet.type !== "string") return;

    const sender = socket.deserializeAttachment() || {};
    // Backward-compatible position relay while older clients are being migrated.
    if (packet.type === "state") {
      if (!Number.isFinite(packet.x) || !Number.isFinite(packet.y) ||
          !Number.isFinite(packet.angle)) return;
      this.broadcast({
        type: "player_state",
        playerId: sender.playerId,
        x: packet.x,
        y: packet.y,
        angle: packet.angle,
        trail: Array.isArray(packet.trail) ? packet.trail.slice(-120) : []
      }, socket);
      return;
    }

    if (packet.type === "match_result") {
      if(typeof packet.eventId!=="string"||packet.eventId.length>120)return;
      const events=await this.ctx.storage.get("leaderboardEvents")||[];
      if(events.some(event=>event.eventId===packet.eventId))return;
      events.push({eventId:packet.eventId,uid:sender.uid||sender.playerId,name:sender.name||"Игрок",avatar:sender.avatar||"",won:packet.won===true,score:Math.max(0,Math.min(100,Math.round(Number(packet.score)||0))),at:Date.now()});
      await this.ctx.storage.put("leaderboardEvents",events); return;
    }
    if (packet.type === "input") {
      if (!Number.isFinite(packet.angle)) return;
      const host = this.hostSocket();
      if (host && host !== socket) {
        try {
          host.send(JSON.stringify({
            type: "player_input",
            playerId: sender.playerId,
            slot: sender.slot,
            angle: Math.max(-Math.PI, Math.min(Math.PI, packet.angle))
          }));
        } catch {}
      }
      return;
    }

    if (packet.type === "start_request" || packet.type === "restart_request") {
      const host = this.hostSocket();
      if (host) {
        try { host.send(JSON.stringify({ type: packet.type, playerId: sender.playerId })); } catch {}
      }
      return;
    }

    if (packet.type === "players_state") {
      if (sender.isHost !== true || !Array.isArray(packet.players) || packet.players.length > 4) return;
      const payload = JSON.stringify({ type: "players_state", players: packet.players });
      if (payload.length > MAX_PACKET_BYTES) return;
      this.broadcast({ type: "players_state", players: packet.players }, socket);
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
