# Security Notes — Signal Check Public Exposure

**Applies to:** `/v1/leads/signal-check`  
**Status:** Preconditions not yet met — endpoint is not cleared for public exposure  
**Owner:** Engineering / Ops  
**Related:** ADR-010

---

## What counts as "public exposure"

**Trigger condition:** The moment a URL is shared outside a trusted internal
environment — including Slack, email, a demo call, or a shared document — it
is considered public exposure.

"It's just a demo link" is not an exception. Once a URL leaves a controlled
environment, you cannot control who receives it, forwards it, or scripts against
it. The checklist below must be complete before any URL is shared externally,
regardless of the intended audience.

---

## Current security posture

| Control | Status | Notes |
|---|---|---|
| In-process rate limiting | ✓ Active | Sandbox grade only — see limitations below |
| Fail-safe IP resolution | ✓ Active | X-Forwarded-For ignored without trusted proxy config |
| PII not leaked in response | ✓ Active | Enforced by ADR-008 + tests |
| No write side effects | ✓ Active | Stateless endpoint |
| Gateway-level rate limiting | ✗ Missing | Required before public exposure |
| WAF / abuse filtering | ✗ Missing | Required before public exposure |
| Edge request logging | ✗ Missing | Required before public exposure |
| IP reputation filtering | ✗ Missing | Required for open internet exposure |

---

## In-process limiter limitations

The current `_TokenBucket` rate limiter is **sandbox-grade only**:

- Does not survive process restarts — bucket resets on every deploy
- Does not coordinate across replicas — each process has its own counter
- Can be bypassed by spoofing `X-Forwarded-For` without `LEG_TRUSTED_PROXY_IPS` configured
- Default: 30 requests / 60 seconds per IP — trivially exhausted by scripted clients

**This limiter is sufficient for:** local testing, controlled demos with known participants  
**This limiter is not sufficient for:** any URL reachable by an untrusted party

---

## Abuse scenarios to mitigate before public exposure

### Enumeration
Attacker iterates over email inputs to map signal rule boundaries (e.g. which
TLDs trigger `suspicious_domain`, which prefixes trigger `shared_inbox`).
**Mitigation:** gateway rate limiting + request pattern detection.

### Scripted replay
Automated client sends thousands of scenario payloads to probe signal logic.
**Mitigation:** gateway rate limiting + bot filtering.

### Probing
Attacker infers internal signal thresholds by observing response differences.
**Mitigation:** WAF anomaly detection + rate limiting per IP/ASN.

### Resource exhaustion
Flood of requests queues behind the single-worker executor, causing 503 storms.
**Mitigation:** gateway rate limiting before requests reach the application.

---

## Public demo exposure checklist

Before sharing any public URL for `/v1/leads/signal-check`:

- [ ] Endpoint is behind a reverse proxy (nginx, Cloudflare, AWS API Gateway, or equivalent)
- [ ] Endpoint is not accessible via direct IP or origin URL (only via gateway hostname)
- [ ] Gateway hostname is the only publicly resolvable endpoint (origin IP is not exposed via DNS)
- [ ] `LEG_TRUSTED_PROXY_IPS` is configured with the proxy IP(s)
- [ ] Gateway-level rate limiting is active (not just in-process)
- [ ] WAF or basic bot filtering is enabled
- [ ] Edge request logging is active and retained
- [ ] `LEG_SIGNAL_CHECK_WORKERS` and `_EVAL_TIMEOUT_SECONDS` are reviewed for expected traffic
- [ ] Direct internet access to the application port is blocked (firewall / security group)
- [ ] Demo URL is time-limited or access-controlled if shared externally

**Do not share a demo link until all checked items above are complete.**

---

## Backlog — Public demo exposure hardening

When preparing for public exposure, implement in this order:

1. **Gateway / reverse proxy** — nginx, Cloudflare, or cloud API gateway in front of signal-check
2. **Gateway-level rate limiting** — outside process, per-IP and per-ASN
3. **WAF rules** — basic bot filtering, request size limits, header validation
4. **IP reputation filtering** — block known abuse sources at edge
5. **Edge request logging** — structured logs at proxy layer with retention
6. **Distributed rate limiter** — replace in-process `_TokenBucket` with Redis-backed limiter (see ADR-002 patterns)
7. **Direct exposure firewall rule** — ensure app port is not directly reachable

Each item above is a separate implementation task. Do not skip steps 1-3.

---

## What stays sandbox-grade (intentionally)

Even after public exposure hardening, the following remain sandbox-grade by design:

- `scenario_id` is not authoritative identity (ADR-010)
- No tenant isolation guarantees
- No audit trail or compliance logging at application level
- No auth boundary

These are not gaps to fix — they are intentional constraints of the validation
surface. If authoritative tenant identity or compliance logging is needed,
implement a separate endpoint with the full ingest contract.
