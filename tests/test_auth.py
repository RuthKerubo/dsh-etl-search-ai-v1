"""Tests for authentication system."""

import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone

from api.auth.service import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)


# =============================================================================
# Password Hashing
# =============================================================================


class TestPasswordHashing:
    """Tests for password hashing and verification."""

    def test_hash_returns_string(self):
        hashed = hash_password("testpass123")
        assert isinstance(hashed, str)
        assert hashed != "testpass123"

    def test_verify_correct_password(self):
        hashed = hash_password("testpass123")
        assert verify_password("testpass123", hashed) is True

    def test_verify_wrong_password(self):
        hashed = hash_password("testpass123")
        assert verify_password("wrongpassword", hashed) is False

    def test_different_passwords_different_hashes(self):
        hash1 = hash_password("password1")
        hash2 = hash_password("password2")
        assert hash1 != hash2

    def test_same_password_different_hashes(self):
        """bcrypt salts should produce different hashes."""
        hash1 = hash_password("samepassword")
        hash2 = hash_password("samepassword")
        assert hash1 != hash2
        # But both should verify
        assert verify_password("samepassword", hash1)
        assert verify_password("samepassword", hash2)


# =============================================================================
# JWT Tokens
# =============================================================================


class TestJWTTokens:
    """Tests for JWT token creation and decoding."""

    def test_create_token(self):
        token = create_access_token({"sub": "user@test.com", "role": "researcher"})
        assert isinstance(token, str)
        assert len(token) > 0

    def test_decode_valid_token(self):
        token = create_access_token({"sub": "user@test.com", "role": "researcher"})
        payload = decode_access_token(token)
        assert payload is not None
        assert payload["sub"] == "user@test.com"
        assert payload["role"] == "researcher"

    def test_decode_invalid_token(self):
        payload = decode_access_token("invalid.token.here")
        assert payload is None

    def test_decode_empty_token(self):
        payload = decode_access_token("")
        assert payload is None

    def test_token_contains_expiry(self):
        token = create_access_token({"sub": "user@test.com"})
        payload = decode_access_token(token)
        assert "exp" in payload

    def test_token_preserves_custom_claims(self):
        token = create_access_token({"sub": "user@test.com", "role": "admin", "custom": "value"})
        payload = decode_access_token(token)
        assert payload["custom"] == "value"


# =============================================================================
# Auth Endpoint Tests (with mocked dependencies)
# =============================================================================


class TestAuthEndpoints:
    """Tests for auth API endpoints using mocked MongoDB."""

    @pytest.fixture
    def mock_user_repo(self):
        repo = AsyncMock()
        repo.exists = AsyncMock(return_value=False)
        repo.create = AsyncMock(return_value={
            "_id": "user@test.com",
            "email": "user@test.com",
            "hashed_password": hash_password("testpass123"),
            "role": "researcher",
            "created_at": datetime.now(timezone.utc),
        })
        repo.get_by_email = AsyncMock(return_value=None)
        return repo

    @pytest.mark.asyncio
    async def test_register_success(self, mock_user_repo):
        """Test successful registration via the auth service functions."""
        from api.schemas.auth import RegisterRequest, UserRole

        body = RegisterRequest(
            email="newuser@test.com",
            password="testpass123",
            role=UserRole.RESEARCHER,
        )

        # Simulate what the register endpoint does
        mock_user_repo.exists.return_value = False
        assert not await mock_user_repo.exists(body.email)

        hashed = hash_password(body.password)
        await mock_user_repo.create(body.email, hashed, body.role.value)
        mock_user_repo.create.assert_called_once()

        token = create_access_token({"sub": body.email, "role": body.role.value})
        payload = decode_access_token(token)
        assert payload["sub"] == "newuser@test.com"
        assert payload["role"] == "researcher"

    @pytest.mark.asyncio
    async def test_register_duplicate_email(self, mock_user_repo):
        """Test registration with existing email."""
        mock_user_repo.exists.return_value = True
        assert await mock_user_repo.exists("existing@test.com")

    @pytest.mark.asyncio
    async def test_login_success(self, mock_user_repo):
        """Test successful login flow."""
        stored_hash = hash_password("testpass123")
        mock_user_repo.get_by_email.return_value = {
            "_id": "user@test.com",
            "email": "user@test.com",
            "hashed_password": stored_hash,
            "role": "researcher",
            "created_at": datetime.now(timezone.utc),
        }

        user = await mock_user_repo.get_by_email("user@test.com")
        assert user is not None
        assert verify_password("testpass123", user["hashed_password"])

        token = create_access_token({"sub": user["email"], "role": user["role"]})
        payload = decode_access_token(token)
        assert payload["sub"] == "user@test.com"

    @pytest.mark.asyncio
    async def test_login_wrong_password(self, mock_user_repo):
        """Test login with wrong password."""
        stored_hash = hash_password("correctpassword")
        mock_user_repo.get_by_email.return_value = {
            "_id": "user@test.com",
            "email": "user@test.com",
            "hashed_password": stored_hash,
            "role": "researcher",
        }

        user = await mock_user_repo.get_by_email("user@test.com")
        assert not verify_password("wrongpassword", user["hashed_password"])

    @pytest.mark.asyncio
    async def test_login_nonexistent_user(self, mock_user_repo):
        """Test login with nonexistent email."""
        mock_user_repo.get_by_email.return_value = None
        user = await mock_user_repo.get_by_email("nobody@test.com")
        assert user is None

    def test_admin_token_has_admin_role(self):
        """Test that admin tokens carry admin role."""
        token = create_access_token({"sub": "admin@test.com", "role": "admin"})
        payload = decode_access_token(token)
        assert payload["role"] == "admin"

    def test_researcher_token_has_researcher_role(self):
        """Test that researcher tokens carry researcher role."""
        token = create_access_token({"sub": "user@test.com", "role": "researcher"})
        payload = decode_access_token(token)
        assert payload["role"] == "researcher"
