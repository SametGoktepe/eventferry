import type { OutboxRecord, PublishableMessage, Serializer } from "./types.js";

/**
 * Turn a persisted outbox record into a broker-ready message: serialize the
 * payload and attach the standard correlation headers. Shared by the polling
 * `Relay` and the streaming relay so both produce byte-identical messages.
 */
export async function buildPublishable(
  record: OutboxRecord,
  serializer: Serializer,
): Promise<PublishableMessage> {
  const value = await serializer.serialize(record);
  const headers: Record<string, string> = {
    ...record.headers,
    "content-type": serializer.contentType,
    "message-id": record.messageId,
    "aggregate-type": record.aggregateType,
    "aggregate-id": record.aggregateId,
  };
  if (record.traceId) headers["trace-id"] = record.traceId;

  return {
    topic: record.topic,
    key: record.key ?? record.aggregateId,
    value,
    headers,
    recordId: record.id,
    messageId: record.messageId,
  };
}
