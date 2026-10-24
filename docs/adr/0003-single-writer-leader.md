# 0003. Single writer: followers proxy writes to the leader

Status: Accepted

## Context

Followers must serve read APIs and be ready for failover. Write requests
(submission, cancel, DLQ retry, and the worker protocol) could either be
executed on any replica, relying on row locks for correctness, or only on
the leader.

## Decision

Only the leader executes writes. A follower reverse-proxies write requests
to the address in `leader_lease.holder_addr`, or answers `503` with
`Retry-After` when there is no live leader. Reads are served by any replica.
Clients and workers can therefore point at any replica or a load balancer.

## Consequences

- One rule for fencing: every event insert carries the current leader's
  token. There are no "unfenced" writers to reason about.
- In-memory leader state (rate-limit buckets, the claim long-poll notifier,
  the inference batcher's view of load) is authoritative because there is
  only one copy.
- Write throughput is one replica's worth. Followers add read capacity and
  availability, not write capacity.
- One extra hop for writes that hit a follower.

## Alternatives considered

- **Any replica writes, leader only runs loops:** more write throughput, but
  rate limits and concurrency limits become distributed problems, and a
  follower write racing a leader loop on the same workflow is correct only if
  every code path takes the right locks. Rejected for now; revisit if the
  leader's CPU becomes the bottleneck in benchmarks.
- **HTTP redirects (307) instead of proxying:** pushes leader discovery into
  every client and worker.
