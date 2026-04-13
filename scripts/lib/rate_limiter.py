"""
TAILOR — Rate limiter for authentication attempts.
Tracks failed auth by IP. Bans IP after max_attempts within window.
In-memory, resets on server restart.
"""

import time
import logging

log = logging.getLogger("tailor.rate_limit")


class AuthRateLimiter:
    """Track failed auth attempts per IP, ban after threshold."""

    def __init__(self, max_attempts: int = 5, window_seconds: int = 900, ban_seconds: int = 1800):
        """
        Args:
            max_attempts: Failed attempts before ban (default 5)
            window_seconds: Time window for counting attempts (default 15 min)
            ban_seconds: Ban duration after threshold (default 30 min)
        """
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.ban_seconds = ban_seconds
        self._attempts: dict[str, list[float]] = {}  # ip -> [timestamps]
        self._bans: dict[str, float] = {}  # ip -> ban_expiry

    def is_banned(self, ip: str) -> bool:
        """Check if IP is currently banned."""
        expiry = self._bans.get(ip)
        if expiry is None:
            return False
        if time.time() > expiry:
            del self._bans[ip]
            self._attempts.pop(ip, None)
            return False
        return True

    def ban_remaining(self, ip: str) -> int:
        """Seconds remaining on ban, 0 if not banned."""
        expiry = self._bans.get(ip, 0)
        remaining = int(expiry - time.time())
        return max(0, remaining)

    def record_failure(self, ip: str) -> bool:
        """Record a failed auth attempt. Returns True if IP is now banned."""
        now = time.time()
        cutoff = now - self.window_seconds

        # Clean old attempts
        attempts = self._attempts.get(ip, [])
        attempts = [t for t in attempts if t > cutoff]
        attempts.append(now)
        self._attempts[ip] = attempts

        if len(attempts) >= self.max_attempts:
            self._bans[ip] = now + self.ban_seconds
            log.warning(f"IP {ip} banned for {self.ban_seconds}s after {len(attempts)} failed auth attempts")
            return True
        return False

    def record_success(self, ip: str):
        """Clear attempts on successful auth."""
        self._attempts.pop(ip, None)
        self._bans.pop(ip, None)

    def get_stats(self) -> dict:
        """Return rate limiter stats for dashboard."""
        now = time.time()
        active_bans = {ip: int(exp - now) for ip, exp in self._bans.items() if exp > now}
        return {
            "active_bans": len(active_bans),
            "banned_ips": active_bans,
            "tracked_ips": len(self._attempts),
            "config": {
                "max_attempts": self.max_attempts,
                "window_seconds": self.window_seconds,
                "ban_seconds": self.ban_seconds,
            },
        }
