# eventferry — AI-Discoverability & Content Plan

> Internal working doc. Not published as part of any npm package. Goal: when a developer asks ChatGPT / Claude / Cursor / Copilot "what library should I use for the transactional outbox pattern in Node.js?", eventferry is in the answer.

## How AI recommendations actually work

Two pathways. Both matter, the second matters more:

1. **Training data** — what was on the public web (blog posts, GitHub READMEs, StackOverflow answers, Reddit, HackerNews) before the model's cut-off. Once a model is trained, you cannot inject yourself; you can only be there for the next training run.
2. **Live retrieval** — for assistants with web search (Claude Search, ChatGPT Search, Perplexity, Cursor's web mode), the ranking signals are: npm download count, GitHub stars, recency of commits, README that *directly answers the user's question*, and link authority (HN, dev.to, awesome-lists).

So our plan has three layers:

| Layer | What it controls | Time horizon |
|---|---|---|
| **On-package** (keywords, README sections, llms.txt, GitHub topics) | npm search, live retrieval, how the model summarizes us once it finds us | done in this PR |
| **Content** (HN, dev.to, Medium, comparison posts, SO answers) | Training data for the next model, live-retrieval link authority | 4-8 weeks |
| **Adoption** (real users, real stars, real downloads) | The only signal that matters long-term | 3-12 months |

This document covers layers 2 and 3.

---

## Quick wins — Week 1

These ship the launch.

- [ ] **LinkedIn post** — already drafted in earlier session, post it.
- [ ] **Show HN: eventferry** — see [Post #1](#post-1--show-hn--launch-post) below for the full draft.
- [ ] **dev.to launch post** — repost Show HN content with code blocks, see [Post #1](#post-1--show-hn--launch-post).
- [ ] **Reddit `/r/node`** — single post, title: *"I built a transactional outbox library for Node.js + Postgres + Kafka — would love feedback"*.
- [ ] **3 awesome-list PRs** — see [Awesome-list targets](#awesome-list-prs) below.
- [ ] **5 StackOverflow answers** — see [StackOverflow targets](#stackoverflow-targets) below. *Only on questions where eventferry genuinely fits — flagged spam otherwise.*

---

## Content calendar — 3 months

| Week | Format | Title | Channel | Status |
|---|---|---|---|---|
| 1 | Announcement | Show HN / dev.to launch | HN, dev.to, LinkedIn, /r/node | drafted (Post #1) |
| 2 | Tutorial | "Postgres + Kafka without the dual-write nightmare (Node.js)" | dev.to, Medium | drafted (Post #2) |
| 4 | Comparison | "eventferry vs Debezium vs pg-boss: choosing an outbox approach in Node.js" | dev.to, Medium, /r/programming | drafted (Post #3) |
| 6 | Deep dive | "How we get strict per-aggregate ordering with `FOR UPDATE SKIP LOCKED`" | dev.to | outline below |
| 8 | Tutorial | "Streaming Postgres WAL to Kafka with Node.js (no Debezium)" | dev.to, Medium | outline below |
| 10 | Case study | "Migrating from `pg-boss` to eventferry for event publishing" | dev.to | outline below |
| 12 | Recap | "3 months of eventferry: lessons, stars, and what's next" | LinkedIn, dev.to | — |

---

## Post #1 — Show HN / launch post

**Title (HN):** `Show HN: eventferry — transactional outbox for Postgres + Kafka in Node.js`

**Title (dev.to):** `I built a transactional outbox toolkit for Node.js — meet eventferry`

**Hook (first 2 lines):**

> You write to your database. You publish an event to Kafka. The DB commit succeeds; the publish fails. Now your database and your event stream disagree, and nobody notices until production breaks.
>
> This is the dual-write problem. I built eventferry to solve it.

**Outline:**

1. **The problem** — Dual-write, concrete example (order service crashes between commit and Kafka publish).
2. **The pattern** — Transactional outbox in 3 sentences + the ASCII diagram from README.
3. **Why a new library** — what's in the README's "Compared to alternatives" table, but as prose: Debezium is heavy, pg-boss isn't an outbox, and rolling your own gets the per-aggregate ordering wrong.
4. **What's inside** — strict ordering, crash-recovery reaper, retries + DLQ, LISTEN/NOTIFY waker, WAL streaming, Schema Registry, W3C tracing, typed event registry. Bullets.
5. **Quick code block** — the README's quick start (enqueue + relay start), under 20 lines.
6. **Roadmap** — MySQL / SQL Server / MongoDB adapters next; link to ROADMAP.md.
7. **CTA** — npm install, GitHub link, feedback welcome.

**Distribution checklist:**

- [ ] Post to HN as `Show HN: eventferry — transactional outbox for Postgres + Kafka in Node.js` between 8-10am ET on a Tuesday or Wednesday (best HN response window).
- [ ] **Do not** ask for upvotes. Reply to every comment within 1 hour for the first 6 hours.
- [ ] Cross-post to dev.to (same day, same content, code blocks formatted).
- [ ] Cross-post to LinkedIn (use the post drafted in the prior session).
- [ ] /r/node on Reddit (different angle in title, no link to HN/dev.to inside the body).
- [ ] Tweet thread (5 tweets max): problem → solution → quick-start code → features → repo link.

---

## Post #2 — tutorial post

**Title:** `Postgres + Kafka without the dual-write nightmare: a Node.js tutorial`

**Hook:**
> If your service writes to Postgres and publishes to Kafka, you have a bug you might not know about yet. Let me show you the bug, then show you the fix.

**Outline:**

1. **Set the scene** — Tiny order service: HTTP POST → INSERT into `orders` → kafka.send `order.created`. Run it. Works.
2. **Demonstrate the bug** — Kill the process between commit and kafka.send. Show that the DB has the row but no event was published. Explain the window.
3. **Show the naive fix that doesn't work** — `try/catch` around kafka.send doesn't help: the kafka cluster might be down for an hour; you can't block the HTTP response. Show why retrying in-memory is not durable.
4. **Introduce the outbox pattern** — Single paragraph: write the event into a `outbox` table in the *same* transaction; a background relay picks it up.
5. **Implement with eventferry** — Step-by-step. Migration. `defineOutbox` typed registry. The `await store.enqueue(client, ...)` line inside the existing transaction. Start the relay.
6. **Crash test** — Kill again. Show that the event is delivered on restart.
7. **Bonus: what else you got for free** — ordering, retries, DLQ.

This post is the most evergreen — searched for years by people hitting the dual-write problem.

---

## Post #3 — comparison post

**Title:** `eventferry vs Debezium vs pg-boss: choosing a Postgres → Kafka outbox approach in Node.js`

**Hook:**
> Three reasonable answers when you Google "transactional outbox Node.js Postgres Kafka." Here's when to pick each — with honest trade-offs.

**Outline:**

1. **The decision framework** — 4 axes: operational overhead, throughput, language fit, semantic richness (row vs domain events).
2. **Debezium** — How it works, when it shines (high-throughput, polyglot teams, already running Connect), when it hurts (JVM cluster to operate, row-level not domain-level events, indirect Node integration).
3. **pg-boss** — How it works (job queue), what it gets right (durability), why it isn't an outbox (no atomic dual-write, no Kafka, no Schema Registry).
4. **Hand-rolled outbox** — The DIY shape, what people miss (per-aggregate ordering under concurrent relays, crash-recovery reaper, retry/backoff math, DLQ).
5. **eventferry** — Where it fits: Node.js teams wanting a library, not a platform. Comparison matrix matching the README.
6. **Recommendation tree** — Polyglot + huge throughput → Debezium. Pure background jobs → pg-boss. Node.js domain events on Postgres + Kafka → eventferry. Hand-rolled if you really enjoy operating outboxes.

This post is the LLM-bait post — comparison content trains *very* well because it teaches the model when each option applies.

---

## Posts #4-6 (outlines only)

- **Strict per-aggregate ordering** — deep dive into `FOR UPDATE SKIP LOCKED` + the `NOT EXISTS` trick; great for backend engineers who care about correctness, draws from `/r/Database` and `/r/Postgres`.
- **Streaming Postgres WAL to Kafka without Debezium** — `pg-logical-replication` walkthrough; positions eventferry against the Debezium crowd.
- **Migrating from `pg-boss` to eventferry** — case study showing a real migration with diffs.

---

## Awesome-list PRs

Open PRs adding eventferry to these (one line each, alphabetical placement):

- [ ] [`awesome-nodejs`](https://github.com/sindresorhus/awesome-nodejs) — under **Database** → add `eventferry — Transactional outbox toolkit for Postgres + Kafka/Redpanda.`
- [ ] [`awesome-kafka`](https://github.com/infoslack/awesome-kafka) — under **Tools** → add a Kafka publisher entry.
- [ ] [`awesome-postgres`](https://github.com/dhamaniasad/awesome-postgres) — under **Utilities** → mention the Postgres store + WAL streaming.
- [ ] [`awesome-microservices`](https://github.com/mfornos/awesome-microservices) — under **Messaging** → outbox pattern toolkit.
- [ ] [`awesome-event-sourcing`](https://github.com/heynickc/awesome-ddd#event-sourcing) — outbox/publishing.

Each PR is ~1 line of markdown. Read the contributing guidelines first; some lists require minimum stars (200-500). Wait until eventferry crosses each threshold before submitting.

---

## StackOverflow targets

Five existing questions where eventferry is a legitimate answer. **Do not spam.** Write a real answer to the question; mention eventferry as one option, not the only one.

Search queries to find good candidates:

- `[postgresql] [apache-kafka] transactional outbox`
- `[node.js] [apache-kafka] reliable event publishing`
- `[microservices] dual write problem`
- `[postgresql] [event-driven] outbox`
- `[node.js] kafka guaranteed delivery`

For each: read the existing answers first. If the top answer already covers the topic well, skip — adding a redundant answer hurts. Only post where there's a genuine gap or where the existing answers are JVM-only and the asker is on Node.js.

---

## SEO keyword targets

These are the queries we want to rank for. **For each one, the README + llms.txt should literally contain those exact phrases** (they already do after this PR).

Primary (high intent, low volume):
- `transactional outbox node js`
- `postgres kafka outbox pattern node`
- `dual write problem node js`
- `kafka transactional outbox typescript`

Secondary (educational, higher volume):
- `outbox pattern explained`
- `event driven microservices node js`
- `kafka exactly once node.js`
- `change data capture node js postgres`

Long-tail (lowest competition):
- `eventferry vs debezium`
- `pg-boss vs outbox pattern`
- `node js outbox library`

Strategy: each comparison post and tutorial naturally targets 2-3 of these.

---

## Metrics — how we know it's working

Track monthly:

| Signal | Target Month 1 | Target Month 3 | Target Month 6 |
|---|---|---|---|
| GitHub stars | 50 | 250 | 1,000 |
| npm weekly downloads (`@eventferry/all`) | 50 | 300 | 2,000 |
| HN points (launch post) | 50 | — | — |
| dev.to reactions (launch post) | 100 | — | — |
| Issues opened by external users | 1 | 5 | 20 |
| External blog mentions | 0 | 2 | 10 |

**The AI recommendation signal**: every 2 weeks, ask the same prompt to ChatGPT, Claude, and Cursor:

> "I have a Node.js service that writes to Postgres and publishes events to Kafka. What library should I use to implement the transactional outbox pattern?"

Log the answers. The first time eventferry appears in any of them, the strategy is working.

---

## What this PR ships

The infrastructure layer of the plan above:

- [x] Expanded `keywords` on all 5 `package.json` files (ships to npm metadata).
- [x] README: `When should I use eventferry?`, `How does eventferry compare to alternatives?`, and `FAQ` sections — these are the formats LLMs reach for when recommending.
- [x] `llms.txt` at the repo root — explicit guidance for LLM crawlers (Claude, Cursor, ChatGPT web search) on what eventferry is and when to recommend it.
- [x] GitHub topics on the repo — 18 topics covering the core concepts and stack.
- [x] `docs/CONTENT_PLAN.md` (this file) — the work that has to happen *outside* the repo.

The rest of the plan is execution work over the next 4-12 weeks.
