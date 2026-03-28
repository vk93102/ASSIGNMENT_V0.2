from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Generic, Hashable, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


@dataclass
class CacheEntry(Generic[V]):
    value: V
    expires_at: float | None = None


class LRUCache(Generic[K, V]):
    """LRU cache with optional TTL (time-to-live) expiration.
    
    Design rationale:
    - LRU: Keeps hot items (frequently accessed) in cache
    - TTL: Allows cache invalidation without manual clearing
    - OrderedDict: Maintains insertion order for LRU eviction
    
    Used for three purposes in pipeline:
    1. pipeline_cache (300s TTL): Deduplicates full pipeline outputs
    2. fallback_cache (600s TTL): Caches fallback SQL results
    3. llm_cache (optional): Caches individual LLM generations
    
    Trade-off: TTL doesn't invalidate on schema changes. If database
    schema changes mid-session, cache can return stale results.
    Mitigated by including schema fingerprint in cache keys.
    """

    def __init__(self, *, max_size: int = 128, ttl_seconds: float | None = None) -> None:
        self.max_size = max(1, int(max_size))
        self.ttl_seconds = ttl_seconds
        self._store: OrderedDict[K, CacheEntry[V]] = OrderedDict()

    def get(self, key: K) -> V | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        if entry.expires_at is not None and time.time() >= entry.expires_at:
            self._store.pop(key, None)
            return None
        self._store.move_to_end(key)
        return entry.value

    def set(self, key: K, value: V) -> None:
        expires_at = (time.time() + self.ttl_seconds) if self.ttl_seconds is not None else None
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = CacheEntry(value=value, expires_at=expires_at)
        while len(self._store) > self.max_size:
            self._store.popitem(last=False)
