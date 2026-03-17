# ADR-006 — Per-Tenant Concurrency Isolation

**Status:** Proposed  
**Date:** 2026-03-17  
**Trigger:** Noisy neighbor test revealed tenant fairness issue under burst conditions

---

## Context

The noisy neighbor benchmark (`benchmarks/noisy_neighbor.py`) revealed that a single
high-volume tenant can degrade other tenants' p99 latency significantly:

```
two_tenant_storm result:
  tenant_noisy  (500 concurrent)  p99 = 2299ms
  tenant_normal (100 requests)    p99 = 2125ms  ← FAIL (exceeds 3x baseline)
```

Root cause: the pipeline uses a shared asyncio event loop and shared Redis connection
pool. A burst from tenant A occupies all execution slots, causing tenant B to queue
behind it even though they are logically independent.

This is expected behavior for a shared-pool architecture, but unacceptable for a
multi-tenant SaaS product where one tenant must not degrade another's SLA.

---

## Decision

Introduce per-tenant concurrency isolation at the pipeline ingestion layer.

### Option A — Per-tenant semaphore (recommended, lowest complexity)

Add a `per_tenant_max_concurrent` field to `TenantConfig`. The pipeline acquires
a per-tenant semaphore before processing each request.

```python
# TenantConfig
per_tenant_max_concurrent: int = 50  # default — tunable per tenant tier

# IngestionPipeline
self._tenant_semaphores: dict[str, asyncio.Semaphore] = {}

def _get_tenant_semaphore(self, tenant_id: str, config: TenantConfig) -> asyncio.Semaphore:
    if tenant_id not in self._tenant_semaphores:
        self._tenant_semaphores[tenant_id] = asyncio.Semaphore(
            config.per_tenant_max_concurrent
        )
    return self._tenant_semaphores[tenant_id]

async def process(self, lead: LeadInput) -> DecisionResult:
    tenant_config = self._tenants.get(lead.tenant_id)
    sem = self._get_tenant_semaphore(lead.tenant_id, tenant_config)
    async with sem:
        ...  # existing pipeline logic
```

**Pros:** minimal code change, deterministic, no new infrastructure  
**Cons:** does not prevent CPU starvation from a tenant that holds many semaphore slots

### Option B — Per-tenant task queue with priority

Each tenant gets its own asyncio queue. A fair scheduler dispatches from queues
in round-robin or weighted-round-robin order.

**Pros:** true fairness, prevents starvation  
**Cons:** significantly more complex, adds scheduling latency overhead

### Option C — Separate worker pools per tenant tier

ENTERPRISE tier tenants get dedicated workers. SMALL/MEDIUM share a pool.

**Pros:** strong SLA guarantees for high-value tenants  
**Cons:** resource intensive, complex routing

---

## Recommendation

Implement **Option A** as the immediate fix. It directly addresses the noisy
neighbor problem with minimal risk and is consistent with the existing `TenantConfig`
extensibility model.

Option B should be evaluated if Option A proves insufficient under production load.
Option C is appropriate when dedicated SLA tiers become a product feature.

---

## Consequences

**After Option A:**
- Each tenant is limited to `per_tenant_max_concurrent` simultaneous pipeline calls
- A noisy tenant with 500 concurrent requests will queue behind the semaphore
- Normal tenants with fewer concurrent requests are unaffected
- Existing `TenantConfig` already has the extension point — one new field

**Tier defaults (proposed):**

| Tier | per_tenant_max_concurrent |
|---|---|
| SMALL | 20 |
| MEDIUM | 50 |
| LARGE | 100 |
| ENTERPRISE | 200 |

**What this does NOT fix:**
- Redis connection pool contention (separate concern — addressed by `redis_max_connections` in settings)
- CPU-bound work within a single request (not applicable — pipeline is I/O bound)

---

## Test plan

After implementation, re-run:

```bash
python benchmarks/noisy_neighbor.py --redis-url redis://localhost:6379 --scenario all
```

Expected outcome: `two_tenant_storm` and `three_tenant_storm` both return `PASS`.

Acceptance criteria: normal tenant p99 < 1.5x baseline (750ms) under 500-request noisy storm.
