"""Auth dependencies for FastAPI dependency injection."""

from typing import Annotated, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.auth.service import decode_access_token
from api.schemas.auth import UserRole

security = HTTPBearer()
security_optional = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> dict:
    """Decode JWT and return user payload."""
    payload = decode_access_token(credentials.credentials)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
    return payload


async def require_admin(
    user: Annotated[dict, Depends(get_current_user)],
) -> dict:
    """Require admin role."""
    if user.get("role") != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return user


async def get_optional_user(
    credentials: Annotated[
        Optional[HTTPAuthorizationCredentials],
        Depends(security_optional),
    ],
) -> Optional[dict]:
    """Decode JWT if present; return None for anonymous requests."""
    if credentials is None:
        return None
    payload = decode_access_token(credentials.credentials)
    return payload  # None if token is invalid — treated as anonymous


CurrentUser = Annotated[dict, Depends(get_current_user)]
AdminUser = Annotated[dict, Depends(require_admin)]
OptionalUser = Annotated[Optional[dict], Depends(get_optional_user)]
