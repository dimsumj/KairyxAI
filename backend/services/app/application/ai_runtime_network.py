from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlsplit


LOCAL_RUNTIME_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
}


def normalize_and_validate_runtime_base_url(
    base_url: str,
    *,
    allow_private_network_hosts: bool,
) -> str:
    normalized = str(base_url or "").strip().rstrip("/")
    parsed = urlsplit(normalized)
    scheme = str(parsed.scheme or "").strip().lower()
    hostname = str(parsed.hostname or "").strip().lower()
    if scheme not in {"http", "https"}:
        raise ValueError("base_url must start with https:// or http://.")
    if not hostname:
        raise ValueError("base_url must include a hostname.")
    if scheme != "https" and not _host_uses_private_network(hostname):
        raise ValueError("Public OpenAI-compatible runtime base_url values must use https://.")
    if _host_uses_private_network(hostname) and not allow_private_network_hosts:
        raise ValueError(
            "Private-network or localhost OpenAI-compatible runtime base_url values are only allowed outside hosted production deployments."
        )
    return normalized


def _host_uses_private_network(hostname: str) -> bool:
    normalized = str(hostname or "").strip().lower()
    if not normalized:
        return False
    if normalized in LOCAL_RUNTIME_HOSTNAMES:
        return True
    ip_value = _parse_ip(normalized)
    if ip_value is not None:
        return _ip_is_privateish(ip_value)
    try:
        resolved = socket.getaddrinfo(normalized, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False
    for candidate in resolved:
        sockaddr = candidate[4] or ()
        if not sockaddr:
            continue
        ip_value = _parse_ip(sockaddr[0])
        if ip_value is not None and _ip_is_privateish(ip_value):
            return True
    return False


def _parse_ip(value: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    try:
        return ipaddress.ip_address(str(value or "").strip())
    except ValueError:
        return None


def _ip_is_privateish(value: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return bool(
        value.is_private
        or value.is_loopback
        or value.is_link_local
        or value.is_reserved
        or value.is_multicast
        or value.is_unspecified
    )
