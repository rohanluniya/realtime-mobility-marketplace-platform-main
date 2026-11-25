"""Security controls for the Mobility Marketplace Serving API (Section 22).

Provides:
- Role-Based Access Control (RBAC): `admin`, `operator`, `analyst`, and `public_aggregated`
  (public dashboard views return ONLY H3-aggregated metrics, never individual GPS traces).
- Per-client sliding/fixed-window rate limiting.
- Secret-safe security audit logging.
"""

from __future__ import annotations

import hmac
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Literal

from fastapi import Depends, HTTPException, Request, status

from marketplace_platform.config import MarketplaceConfig, load_config
from marketplace_platform.observability import get_logger

Role = Literal["admin", "operator", "analyst", "public_aggregated"]

ROLE_RANK: dict[Role, int] = {
    "public_aggregated": 0,
    "analyst": 1,
    "operator": 2,
    "admin": 3,
}


@dataclass(frozen=True)
class Principal:
    role: Role
    client_ip: str
    authenticated: bool


class RateLimiter:
    """In-memory per-IP fixed-window rate limiter."""

    def __init__(self, max_requests: int = 600, window_seconds: int = 60) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, list[float]] = defaultdict(list)

    def check(self, client_key: str) -> None:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        bucket = [t for t in self._hits[client_key] if t >= cutoff]
        if len(bucket) >= self.max_requests:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded; retry shortly",
            )
        bucket.append(now)
        self._hits[client_key] = bucket


_rate_limiter = RateLimiter(max_requests=1200, window_seconds=60)
_audit_logger = get_logger("marketplace-api", "security")


def _extract_bearer(request: Request) -> str | None:
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    api_key = request.headers.get("x-api-key")
    return api_key.strip() if api_key else None


def resolve_principal(request: Request, config: MarketplaceConfig | None = None) -> Principal:
    cfg = config or load_config()
    ip = request.client.host if request.client else "unknown"
    _rate_limiter.check(ip)

    token = _extract_bearer(request)
    if not token:
        return Principal(role="public_aggregated", client_ip=ip, authenticated=False)

    if hmac.compare_digest(token, cfg.api_admin_token):
        return Principal(role="admin", client_ip=ip, authenticated=True)
    if hmac.compare_digest(token, cfg.api_operator_token):
        return Principal(role="operator", client_ip=ip, authenticated=True)
    if hmac.compare_digest(token, cfg.api_analyst_token):
        return Principal(role="analyst", client_ip=ip, authenticated=True)

    _audit_logger.warning(
        "security.auth_failed",
        "Rejected invalid API credential",
        error_code="AUTH_INVALID_TOKEN",
        context={"client_ip": ip, "path": request.url.path},
    )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid bearer token or API key",
    )


def require_role(min_role: Role):
    """FastAPI dependency enforcing minimum RBAC role."""

    def _dep(request: Request) -> Principal:
        principal = resolve_principal(request)
        if ROLE_RANK[principal.role] < ROLE_RANK[min_role]:
            _audit_logger.warning(
                "security.rbac_denied",
                "Forbidden RBAC access attempt",
                error_code="RBAC_FORBIDDEN",
                context={
                    "client_ip": principal.client_ip,
                    "role": principal.role,
                    "required_role": min_role,
                    "path": request.url.path,
                },
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{min_role}' or higher required",
            )
        return principal

    return Depends(_dep)
