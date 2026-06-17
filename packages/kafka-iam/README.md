# @eventferry/kafka-iam

[![npm](https://img.shields.io/npm/v/@eventferry/kafka-iam.svg)](https://www.npmjs.com/package/@eventferry/kafka-iam)

**AWS MSK IAM** authentication for [`@eventferry/kafka`](../kafka). MSK exposes IAM auth via SASL/OAUTHBEARER — the bearer token is an AWS Signature V4 signature, not a JWT. This helper wraps the official `aws-msk-iam-sasl-signer-js` library and gives you a one-liner `sasl` block for the publisher.

## Install

```bash
npm i @eventferry/kafka-iam aws-msk-iam-sasl-signer-js
```

`aws-msk-iam-sasl-signer-js` is an optional peer (the AWS-official SigV4 signer).

## Usage

```ts
import { KafkaPublisher } from "@eventferry/kafka";
import { createMskIamSasl } from "@eventferry/kafka-iam";

const publisher = new KafkaPublisher({
  brokers: ["b-1.cluster.us-east-1.amazonaws.com:9098"],
  ssl: true,
  sasl: createMskIamSasl({ region: "us-east-1" }),
});

await publisher.connect();
```

With a named AWS profile or an assumed IAM role:

```ts
createMskIamSasl({ region: "us-east-1", awsProfile: "prod" });

createMskIamSasl({
  region: "us-east-1",
  awsRoleArn: "arn:aws:iam::123456789012:role/eventferry-publisher",
  awsRoleSessionName: "eventferry-prod",
});
```

## Token caching & refresh

MSK IAM tokens have a 15-minute lifetime. This helper memoizes the most recent token and refreshes it shortly before expiry. Both kafkajs and librdkafka invoke the provider on demand (no fixed timer), so process-local caching is the natural place:

- Default `refreshAheadMs: 60_000` — token is regenerated when its remaining lifetime drops below 60 seconds.
- Concurrent invocations during a refresh dedupe onto a single in-flight `generateAuthToken` call (no thundering herd at the SigV4 signer).
- Transient signer failures clear the in-flight slot so the next call retries cleanly.

Tighten or loosen the refresh window:

```ts
createMskIamSasl({ region: "us-east-1", refreshAheadMs: 120_000 });
```

## Custom signer (DI)

For tests or non-standard signers, inject your own:

```ts
createMskIamSasl({
  region: "us-east-1",
  signer: {
    async generateAuthToken({ region }) {
      return { token: await mySignerFor(region), expiryTime: Date.now() + 15 * 60 * 1000 };
    },
  },
});
```

The helper does not dynamically import `aws-msk-iam-sasl-signer-js` when a `signer` is supplied.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
