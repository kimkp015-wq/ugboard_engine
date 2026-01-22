# src/application/services/auth_service.py
import time
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AuthResult(Enum):
    VALID = "valid"
    INVALID_TOKEN = "invalid_token"
    EXPIRED = "expired"
    RATE_LIMITED = "rate_limited"
    DISABLED = "disabled"


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    reason: AuthResult
    remaining_quota: Optional[int] = None
    retry_after: Optional[int] = None


class CredentialRepository(ABC):
    @abstractmethod
    def get_credential(self, platform: str, credential_hash: str) -> Optional[Dict]:
        pass
    
    @abstractmethod
    def update_failure_count(self, platform: str, credential_hash: str, count: int) -> None:
        pass
    
    @abstractmethod
    def disable_credential(self, platform: str, credential_hash: str, reason: str) -> None:
        pass


class RateLimiter(ABC):
    @abstractmethod
    def check_limit(self, key: str, max_requests: int, window_seconds: int) -> Tuple[bool, int]:
        pass


class TokenValidator(ABC):
    @abstractmethod
    def validate_token(self, platform: str, token: str) -> Tuple[bool, Optional[Dict]]:
        pass


class AuthenticationService:
    """Production-ready authentication service with failure tracking and circuit breaking"""
    
    def __init__(
        self,
        credential_repo: CredentialRepository,
        rate_limiter: RateLimiter,
        token_validator: TokenValidator,
        max_failures: int = 5,
        failure_window: int = 300,  # 5 minutes
        lockout_duration: int = 900,  # 15 minutes
        enable_circuit_breaker: bool = True
    ):
        self.credential_repo = credential_repo
        self.rate_limiter = rate_limiter
        self.token_validator = token_validator
        self.max_failures = max_failures
        self.failure_window = failure_window
        self.lockout_duration = lockout_duration
        self.enable_circuit_breaker = enable_circuit_breaker
        self._failure_counts: Dict[str, Dict[str, int]] = {}  # platform -> credential_hash -> count
        self._lockouts: Dict[str, float] = {}  # lockout_key -> expiry_time
        
    def _generate_credential_hash(self, platform: str, token: str) -> str:
        """Generate deterministic hash for credential tracking"""
        salted = f"{platform}:{token}:{self._get_salt()}"
        return hashlib.sha256(salted.encode()).hexdigest()
    
    def _get_salt(self) -> str:
        """In production, this should come from secure config"""
        return "secure_salt_change_in_production"
    
    def authenticate_request(
        self,
        platform: str,
        token: Optional[str],
        client_ip: Optional[str] = None
    ) -> ValidationResult:
        """
        Authenticate a scraping request with comprehensive validation.
        
        Time Complexity: O(1) for hash operations + O(1) for Redis lookups
        Space Complexity: O(n) for tracking active failures, where n is concurrent failing credentials
        """
        
        # 1. Check for missing credentials
        if not token:
            logger.warning(f"Missing credentials for {platform}")
            return ValidationResult(
                is_valid=False,
                reason=AuthResult.INVALID_TOKEN
            )
        
        credential_hash = self._generate_credential_hash(platform, token)
        lockout_key = f"lockout:{platform}:{credential_hash}"
        
        # 2. Check if credential is currently locked out
        if self.enable_circuit_breaker:
            current_time = time.time()
            lockout_expiry = self._lockouts.get(lockout_key, 0)
            if current_time < lockout_expiry:
                retry_after = int(lockout_expiry - current_time)
                logger.warning(f"Credential locked out for {platform}, retry after {retry_after}s")
                return ValidationResult(
                    is_valid=False,
                    reason=AuthResult.RATE_LIMITED,
                    retry_after=retry_after
                )
        
        # 3. Apply rate limiting by IP and credential
        if client_ip:
            ip_key = f"rate_limit:ip:{client_ip}"
            ip_allowed, ip_remaining = self.rate_limiter.check_limit(
                ip_key, max_requests=100, window_seconds=3600
            )
            if not ip_allowed:
                logger.warning(f"IP rate limit exceeded: {client_ip}")
                return ValidationResult(
                    is_valid=False,
                    reason=AuthResult.RATE_LIMITED,
                    retry_after=60
                )
        
        # 4. Check credential-specific rate limiting
        cred_key = f"rate_limit:cred:{credential_hash}"
        cred_allowed, cred_remaining = self.rate_limiter.check_limit(
            cred_key, max_requests=50, window_seconds=300
        )
        if not cred_allowed:
            logger.warning(f"Credential rate limit exceeded for {platform}")
            return ValidationResult(
                is_valid=False,
                reason=AuthResult.RATE_LIMITED,
                retry_after=30
            )
        
        # 5. Validate token with external service (with timeout)
        try:
            is_valid, validation_data = self.token_validator.validate_token(platform, token)
        except Exception as e:
            logger.error(f"Token validation failed for {platform}: {str(e)}")
            # Fail open or closed based on security requirements
            # For scraping, might want to fail closed
            return ValidationResult(
                is_valid=False,
                reason=AuthResult.INVALID_TOKEN
            )
        
        if not is_valid:
            # 6. Track authentication failures
            self._track_authentication_failure(platform, credential_hash)
            
            logger.warning(f"Invalid {platform} token")
            return ValidationResult(
                is_valid=False,
                reason=AuthResult.INVALID_TOKEN
            )
        
        # 7. Successful authentication - reset failure counter
        self._reset_failure_count(platform, credential_hash)
        
        logger.info(f"Successful authentication for {platform}")
        return ValidationResult(
            is_valid=True,
            reason=AuthResult.VALID,
            remaining_quota=cred_remaining
        )
    
    def _track_authentication_failure(self, platform: str, credential_hash: str) -> None:
        """Track consecutive failures and implement circuit breaker"""
        key = f"{platform}:{credential_hash}"
        
        # Get current failure count
        current_count = self._failure_counts.get(key, {}).get('count', 0)
        last_failure = self._failure_counts.get(key, {}).get('timestamp', 0)
        current_time = time.time()
        
        # Reset if outside failure window
        if current_time - last_failure > self.failure_window:
            current_count = 0
        
        # Increment failure count
        current_count += 1
        self._failure_counts[key] = {
            'count': current_count,
            'timestamp': current_time
        }
        
        # Update in persistent storage
        self.credential_repo.update_failure_count(platform, credential_hash, current_count)
        
        # Activate circuit breaker if threshold exceeded
        if current_count >= self.max_failures:
            lockout_key = f"lockout:{platform}:{credential_hash}"
            self._lockouts[lockout_key] = current_time + self.lockout_duration
            
            logger.warning(
                f"Credential locked out for {platform} after {current_count} failures. "
                f"Lockout duration: {self.lockout_duration}s"
            )
            
            # Disable in persistent storage
            self.credential_repo.disable_credential(
                platform,
                credential_hash,
                f"Exceeded {self.max_failures} failures in {self.failure_window}s"
            )
    
    def _reset_failure_count(self, platform: str, credential_hash: str) -> None:
        """Reset failure counter on successful authentication"""
        key = f"{platform}:{credential_hash}"
        if key in self._failure_counts:
            del self._failure_counts[key]
        self.credential_repo.update_failure_count(platform, credential_hash, 0)
