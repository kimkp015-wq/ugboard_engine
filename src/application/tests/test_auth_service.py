# tests/application/services/test_auth_service.py
import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from src.application.services.auth_service import (
    AuthenticationService,
    AuthResult,
    ValidationResult
)


class MockCredentialRepository:
    def __init__(self):
        self.credentials = {}
        self.failure_counts = {}
    
    def get_credential(self, platform, credential_hash):
        return self.credentials.get(f"{platform}:{credential_hash}")
    
    def update_failure_count(self, platform, credential_hash, count):
        self.failure_counts[f"{platform}:{credential_hash}"] = count
    
    def disable_credential(self, platform, credential_hash, reason):
        cred = self.credentials.get(f"{platform}:{credential_hash}")
        if cred:
            cred['disabled'] = True
            cred['disabled_reason'] = reason


class MockRateLimiter:
    def __init__(self, allowed=True, remaining=100):
        self.allowed = allowed
        self.remaining = remaining
    
    def check_limit(self, key, max_requests, window_seconds):
        return self.allowed, self.remaining


class MockTokenValidator:
    def __init__(self, valid=True):
        self.valid = valid
    
    def validate_token(self, platform, token):
        return self.valid, {"quota": 100} if self.valid else None


@pytest.fixture
def auth_service():
    cred_repo = MockCredentialRepository()
    rate_limiter = MockRateLimiter(allowed=True)
    token_validator = MockTokenValidator(valid=True)
    
    return AuthenticationService(
        credential_repo=cred_repo,
        rate_limiter=rate_limiter,
        token_validator=token_validator,
        max_failures=3,
        failure_window=60,
        lockout_duration=120
    )


def test_authenticate_missing_token(auth_service):
    """Test authentication with missing token"""
    result = auth_service.authenticate_request("youtube", None, "192.168.1.1")
    
    assert not result.is_valid
    assert result.reason == AuthResult.INVALID_TOKEN


def test_authenticate_valid_token(auth_service):
    """Test successful authentication"""
    result = auth_service.authenticate_request("youtube", "valid_token_123", "192.168.1.1")
    
    assert result.is_valid
    assert result.reason == AuthResult.VALID


def test_authenticate_invalid_token(auth_service):
    """Test authentication with invalid token"""
    # Configure token validator to return invalid
    auth_service.token_validator = MockTokenValidator(valid=False)
    
    result = auth_service.authenticate_request("youtube", "invalid_token", "192.168.1.1")
    
    assert not result.is_valid
    assert result.reason == AuthResult.INVALID_TOKEN


def test_rate_limiting_exceeded(auth_service):
    """Test rate limiting"""
    # Configure rate limiter to deny
    auth_service.rate_limiter = MockRateLimiter(allowed=False)
    
    result = auth_service.authenticate_request("youtube", "valid_token", "192.168.1.1")
    
    assert not result.is_valid
    assert result.reason == AuthResult.RATE_LIMITED
    assert result.retry_after is not None


def test_circuit_breaker_activation(auth_service):
    """Test circuit breaker after multiple failures"""
    auth_service.token_validator = MockTokenValidator(valid=False)
    
    # First 2 failures
    for _ in range(2):
        result = auth_service.authenticate_request("youtube", "bad_token", "192.168.1.1")
        assert result.reason == AuthResult.INVALID_TOKEN
    
    # 3rd failure should trigger circuit breaker
    result = auth_service.authenticate_request("youtube", "bad_token", "192.168.1.1")
    
    assert not result.is_valid
    assert result.retry_after is not None


def test_failure_count_reset_on_success(auth_service):
    """Test that failure count resets after successful auth"""
    # First, cause 2 failures
    auth_service.token_validator = MockTokenValidator(valid=False)
    for _ in range(2):
        auth_service.authenticate_request("youtube", "bad_token", "192.168.1.1")
    
    # Now succeed
    auth_service.token_validator = MockTokenValidator(valid=True)
    result = auth_service.authenticate_request("youtube", "good_token", "192.168.1.1")
    
    assert result.is_valid
    assert result.reason == AuthResult.VALID
    # Failure count should be reset


@pytest.mark.parametrize("platform,token,expected", [
    ("youtube", "token1", "hash1"),
    ("twitch", "token2", "hash2"),
])
def test_credential_hashing_consistency(auth_service, platform, token, expected):
    """Test that credential hashing is deterministic"""
    with patch.object(auth_service, '_get_salt', return_value="test_salt"):
        hash1 = auth_service._generate_credential_hash(platform, token)
        hash2 = auth_service._generate_credential_hash(platform, token)
        
        assert hash1 == hash2
        assert isinstance(hash1, str)
        assert len(hash1) == 64  # SHA-256 hex length
