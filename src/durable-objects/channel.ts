import { Hono } from "hono";
import { DurableObject } from "cloudflare:workers";
import { Browsable } from "@outerbase/browsable-durable-object";
import { Env } from "../types/env";

interface ChannelMetadata {
    channel: string;
}

const CONNECTIONS_LIMIT = 10_000
const MAX_SHARD_COUNT = 5

@Browsable()
export class ChannelDurableObject extends DurableObject<Env> {
    // Hono application instance for serving routes
    private app: Hono = new Hono();
    // Map of connections between the USER <-> CHANNEL (this) durable objects. These do not hibernate.
    private connections = new Map<string, WebSocket>();
    // Which shard version of this channel are we using currently
    private shardVersion: number = 0;
    // Marks which channel this object represents
    private channel: string | undefined = undefined;

    constructor(ctx: DurableObjectState, env: Env) {
        super(ctx, env);
    }

    async init(version: number, metadata: ChannelMetadata): Promise<boolean> {
        const { channel } = metadata;
        this.shardVersion = version;
        this.channel = channel;

        return true;
    }

    async webSocketMessage(ws: WebSocket, message: any) {
        // When a message is received by a USER, just echo back to the USER
        // a confirmation message noting that this particularly channel has
        // received that message.
        ws.send(`[CHANNEL - ${this.channel}-${this.shardVersion}]: Received message from [USER]: ${message}`);
    }

    async webSocketClose(
        ws: WebSocket,
        code: number,
        reason: string,
        wasClean: boolean
    ) {
        // When a particular user has ended its websocket connection, we should 
        // find their entry in our connections map and prune it from our list we are
        // managing.
        for (const [id, socket] of this.connections.entries()) {
            if (socket === ws) {
                this.connections.delete(id);
                break;
            }
        }

        ws.close(code, "Durable Object is closing WebSocket");
    }

    async fetch(request: Request): Promise<Response> {
        const url = new URL(request.url)

        // Establish a websocket connection between USER <-> CHANNEL durable object (this).
        // While the incoming nature of this websocket _is_ hibernatable (see: `acceptWebSocket`)
        // rather than just `accept`), the outgoing aspect of the USER durable object makes
        // its socket non-hibernatable.
        if (url.pathname === '/ws') {
            const webSocketPair = new WebSocketPair();
            const [client, server] = Object.values(webSocketPair);

            // Assign a random identifier to the socket and save the pair locally.
            const connectionId = crypto.randomUUID();
            this.connections.set(connectionId, server);
            this.ctx.acceptWebSocket(server);

            return new Response(null, {
                status: 101,
                webSocket: client,
            });
        }

        return this.app.fetch(request);
    }

    public async canSupportConnection(): Promise<{ success: boolean, limitReached?: boolean }> {
        if (this.connections.size < CONNECTIONS_LIMIT) {
            return { success: true }
        } else if (this.connections.size >= CONNECTIONS_LIMIT && this.shardVersion < MAX_SHARD_COUNT) {
            return { success: false }
        }

        return { 
            success: false,
            limitReached: (this.shardVersion + 1) === MAX_SHARD_COUNT
        }
    }
} 