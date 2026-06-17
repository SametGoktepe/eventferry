# @eventferry/kafka-iam

## 0.2.0

### Minor Changes

- c51fb73: New package: `@eventferry/kafka-iam` — AWS MSK IAM (SASL/OAUTHBEARER over SigV4) helper.

  `createMskIamSasl({ region })` returns a ready-to-use `sasl` block for `KafkaPublisher` backed by the official `aws-msk-iam-sasl-signer-js` library (optional peer). Supports named profiles, assumed IAM roles, and custom signer DI for tests. Tokens are cached process-locally with a configurable refresh-ahead window (default 60 s on the 15-minute MSK token lifetime), and concurrent refresh attempts dedupe onto a single in-flight `generateAuthToken` call. Transient signer failures clear the in-flight slot so the next call retries cleanly.

  Zero changes to `@eventferry/kafka` — the helper plugs into the existing `SaslOauthbearerConfig` surface.

### Patch Changes

- Updated dependencies [3e0c5ee]
- Updated dependencies [ac7a964]
- Updated dependencies [4007a8e]
  - @eventferry/kafka@3.4.0
