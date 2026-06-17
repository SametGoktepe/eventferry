---
"@eventferry/kafka": patch
---

Hardened TLS configuration documentation. No API change — `ssl.ca`, `ssl.servername`, and the rest of `TlsConfig` were already on the surface. This round:

- Expanded the `TlsConfig` JSDoc with the driver-parity gap: `servername` is honored by the **kafkajs** driver (Node `tls.connect` reads it directly) but is a documented **no-op on the confluent driver** — librdkafka v1.x's kafkaJS-compat layer doesn't expose an SNI override.
- README gained explicit "Dev cluster with a self-signed cert" and "IP-literal brokers (cert hostname mismatch)" sections with copy-paste examples covering CA pinning + `servername` for SNI/SAN alignment.
- Reaffirmed that `rejectUnauthorized: false` is **never** going to ship on this surface. TLS verification is non-negotiable. For dev clusters with self-signed certs, the supported pattern is to pass the cluster CA via `ssl.ca` so verification still happens — just against your CA instead of the system trust store.

Companion library updates (changesets, dependabot) on the way; this patch only touches comments + README, so the change is safe to consume immediately.
