"""Auth router — register, login, me."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from api.auth.dependencies import CurrentUser
from api.auth.service import create_access_token, hash_password, verify_password
from api.schemas.auth import (
    LoginRequest,
    RegisterRequest,
    TokenResponse,
    UserResponse,
)
from etl.repository.user_repository_mongo import UserRepositoryMongo

router = APIRouter(prefix="/auth", tags=["auth"])


def _get_user_repo() -> UserRepositoryMongo:
    from api.dependencies import get_user_repository
    return get_user_repository()


UserRepoDep = Annotated[UserRepositoryMongo, Depends(_get_user_repo)]


@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterRequest, repo: UserRepoDep):
    """Register a new user and return a JWT."""
    if await repo.exists(body.email):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        )
    hashed = hash_password(body.password)
    await repo.create(body.email, hashed, body.role.value)
    token = create_access_token({"sub": body.email, "role": body.role.value})
    return TokenResponse(access_token=token)


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, repo: UserRepoDep):
    """Authenticate and return a JWT."""
    user = await repo.get_by_email(body.email)
    if user is None or not verify_password(body.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    token = create_access_token({"sub": user["email"], "role": user["role"]})
    return TokenResponse(access_token=token)


@router.get("/me", response_model=UserResponse)
async def me(current_user: CurrentUser, repo: UserRepoDep):
    """Return current user info."""
    user = await repo.get_by_email(current_user["sub"])
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return UserResponse(
        email=user["email"],
        role=user["role"],
        created_at=user["created_at"],
    )
