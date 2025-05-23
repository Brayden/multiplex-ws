export type Env = {
    USER_DURABLE_OBJECT: DurableObjectNamespace;
    CHANNEL_DURABLE_OBJECT: DurableObjectNamespace<
        import('../durable-objects/channel').ChannelDurableObject
    >
} 