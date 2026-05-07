#!/usr/bin/env python3
"""Reserve a desired Yandex Cloud public IPv4 address by trying candidates.

The script is intentionally conservative around destructive actions:
cloud deletion requires both config.allow_delete_cloud=true and the
--yes-delete-cloud CLI flag. Keep the service account in a stable cloud that
this script will not delete.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import ipaddress
import json
import logging
import os
import random
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path
from collections import deque
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional, Set, Tuple


IAM_TOKEN_URL = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
OPERATION_URL = "https://operation.api.cloud.yandex.net/operations/{operation_id}"
RESOURCE_MANAGER_URL = "https://resource-manager.api.cloud.yandex.net/resource-manager/v1"
BILLING_URL = "https://billing.api.cloud.yandex.net/billing/v1"
VPC_URL = "https://vpc.api.cloud.yandex.net/vpc/v1"
IMMEDIATE_DELETE_AFTER = "1970-01-01T00:00:00Z"
SUCCESS_VIDEO_URL = "https://www.youtube.com/watch?v=tiCIjTNARX8&list=PLCZl9PrJVBkSJGJi3zpDkbxy8X-BeUQvK"

DEFAULT_TARGET_CIDRS = [
    "84.201.0.0/16",
    "95.161.0.0/16",
    "130.193.0.0/16",
]

LOGGER = logging.getLogger("yc-ip-hunter")


class ConfigError(RuntimeError):
    """Raised when local configuration is incomplete or invalid."""


class ApiError(RuntimeError):
    """Yandex Cloud API error with enough context for classification."""

    def __init__(
        self,
        status: int,
        code: Any,
        message: str,
        details: Any = None,
        body: Any = None,
    ) -> None:
        super().__init__(f"HTTP {status}, code={code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.details = details
        self.body = body

    def text(self) -> str:
        return " ".join(
            [
                str(self.status),
                str(self.code),
                self.message or "",
                json.dumps(self.details, ensure_ascii=True, default=str)
                if self.details is not None
                else "",
                json.dumps(self.body, ensure_ascii=True, default=str)
                if self.body is not None
                else "",
            ]
        ).lower()


class QuotaHit(RuntimeError):
    """Raised when API indicates a hard quota block."""


class RateLimitHit(RuntimeError):
    """Raised when API asks us to slow down."""


@dataclasses.dataclass
class AttemptResult:
    ip: str
    zone: str
    address_id: str
    cloud_id: str
    folder_id: str
    dry_run: bool = False


def utc_now_rfc3339() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def load_json_or_yaml(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig")
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise ConfigError(
                "YAML config requires PyYAML. Install it or use JSON config."
            ) from exc
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    if not isinstance(data, dict):
        raise ConfigError("Config root must be an object.")
    return data


def resolve_path(base: Path, value: Optional[str]) -> Optional[Path]:
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return (base / path).resolve()


def save_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(path)


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1}
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"State file is not valid JSON: {path}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"State file root must be an object: {path}")
    return data


def setup_logging(log_file: Optional[Path], verbose: bool) -> None:
    LOGGER.setLevel(logging.DEBUG if verbose else logging.INFO)
    LOGGER.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    stream.setLevel(logging.DEBUG if verbose else logging.INFO)
    LOGGER.addHandler(stream)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        LOGGER.addHandler(file_handler)


def config_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)


def youtube_video_parts(url: str) -> Tuple[str, str]:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query)
    video_id = str((query.get("v") or [""])[0])
    playlist_id = str((query.get("list") or [""])[0])
    if not video_id and parsed.netloc.lower().endswith("youtu.be"):
        video_id = parsed.path.strip("/").split("/")[0]
    return video_id, playlist_id


def build_success_video_launcher(url: str) -> str:
    video_id, playlist_id = youtube_video_parts(url)
    if not video_id:
        return url

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>ip hunter</title>
  <style>
    html, body, #player {{
      width: 100%;
      height: 100%;
      margin: 0;
      background: #000;
      overflow: hidden;
    }}
  </style>
</head>
<body>
  <div id="player"></div>
  <script src="https://www.youtube.com/iframe_api"></script>
  <script>
    const videoId = {json.dumps(video_id)};
    const listId = {json.dumps(playlist_id)};
    let player;

    function onYouTubeIframeAPIReady() {{
      const playerVars = {{
        autoplay: 1,
        controls: 1,
        rel: 0,
        playsinline: 1,
        origin: window.location.origin
      }};
      if (listId) {{
        playerVars.listType = "playlist";
        playerVars.list = listId;
      }}
      player = new YT.Player("player", {{
        width: "100%",
        height: "100%",
        videoId,
        playerVars,
        events: {{
          onReady: function(event) {{
            event.target.unMute();
            event.target.setVolume(100);
            event.target.playVideo();
          }}
        }}
      }});
    }}
  </script>
</body>
</html>
"""
    path = Path(tempfile.gettempdir()) / "yc-ip-hunter-success.html"
    path.write_text(html, encoding="utf-8")
    return path.as_uri()


def build_jwt_from_service_account_key(key_path: Path) -> str:
    try:
        import jwt  # type: ignore
    except ImportError as exc:
        raise ConfigError(
            "Service-account key auth requires PyJWT and cryptography. "
            "Install with: python -m pip install PyJWT cryptography"
        ) from exc

    key_data = json.loads(key_path.read_text(encoding="utf-8"))
    private_key = key_data["private_key"]
    key_id = key_data["id"]
    service_account_id = key_data["service_account_id"]

    now = int(time.time())
    payload = {
        "aud": IAM_TOKEN_URL,
        "iss": service_account_id,
        "iat": now,
        "exp": now + 3600,
    }
    token = jwt.encode(
        payload,
        private_key,
        algorithm="PS256",
        headers={"kid": key_id, "typ": "JWT"},
    )
    if isinstance(token, bytes):
        return token.decode("ascii")
    return token


class TokenProvider:
    def __init__(self, config: Dict[str, Any], config_dir: Path) -> None:
        auth = config.get("auth") or {}
        if not isinstance(auth, dict):
            raise ConfigError("auth must be an object.")
        self.iam_token_env = str(auth.get("iam_token_env") or "YC_IAM_TOKEN")
        self.inline_iam_token = auth.get("iam_token")
        self.key_path = resolve_path(config_dir, auth.get("service_account_key_file"))
        self.service_account_id = auth.get("service_account_id")
        if not self.service_account_id and self.key_path and self.key_path.exists():
            try:
                key_data = json.loads(self.key_path.read_text(encoding="utf-8-sig"))
                self.service_account_id = key_data.get("service_account_id")
            except Exception:
                self.service_account_id = None
        self._cached_token: Optional[str] = None
        self._expires_at = 0.0

    def get(self) -> str:
        env_token = os.getenv(self.iam_token_env)
        if env_token:
            return env_token
        if self.inline_iam_token:
            return str(self.inline_iam_token)
        now = time.time()
        if self._cached_token and now < self._expires_at - 600:
            return self._cached_token
        if not self.key_path:
            raise ConfigError(
                "Set auth.service_account_key_file or provide an IAM token via "
                f"{self.iam_token_env}."
            )
        jwt_token = build_jwt_from_service_account_key(self.key_path)
        response = http_json("POST", IAM_TOKEN_URL, body={"jwt": jwt_token}, token=None)
        iam_token = response.get("iamToken")
        if not iam_token:
            raise ApiError(200, "bad_response", "IAM token response has no iamToken", body=response)
        self._cached_token = str(iam_token)
        self._expires_at = now + 3600
        return self._cached_token


def http_json(
    method: str,
    url: str,
    body: Optional[Dict[str, Any]],
    token: Optional[str],
    timeout: int = 60,
) -> Dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        parsed: Any
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"message": raw}
        error_obj = parsed.get("error") if isinstance(parsed, dict) else None
        if not isinstance(error_obj, dict):
            error_obj = parsed if isinstance(parsed, dict) else {}
        raise ApiError(
            exc.code,
            error_obj.get("code"),
            str(error_obj.get("message") or raw or exc.reason),
            details=error_obj.get("details"),
            body=parsed,
        ) from exc
    except urllib.error.URLError as exc:
        raise ApiError(0, "network_error", str(exc.reason)) from exc


class YandexCloudClient:
    def __init__(
        self,
        token_provider: TokenProvider,
        dry_run: bool,
        operation_timeout_seconds: int,
        operation_poll_seconds: float,
        dry_run_allocated_ip: str = "203.0.113.10",
    ) -> None:
        self.token_provider = token_provider
        self.dry_run = dry_run
        self.operation_timeout_seconds = operation_timeout_seconds
        self.operation_poll_seconds = operation_poll_seconds
        self.dry_run_allocated_ip = dry_run_allocated_ip

    def request(
        self,
        method: str,
        url: str,
        body: Optional[Dict[str, Any]] = None,
        operation_hint: str = "operation",
    ) -> Dict[str, Any]:
        if self.dry_run and method != "GET":
            LOGGER.info("[dry-run] %s %s", method, url)
            if body is not None:
                LOGGER.info("[dry-run] body=%s", json.dumps(body, ensure_ascii=True))
            return {
                "id": f"dry-{operation_hint}",
                "done": True,
                "response": {"id": f"dry-{operation_hint}"},
            }
        if self.dry_run:
            LOGGER.info("[dry-run] GET %s", url)
        return http_json(method, url, body=body, token=self.token_provider.get())

    def wait_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        operation_id = operation.get("id")
        if not operation_id:
            raise ApiError(200, "bad_response", "Operation response has no id", body=operation)
        if self.dry_run:
            return operation.get("response") or {}

        deadline = time.time() + self.operation_timeout_seconds
        current = operation
        while True:
            if current.get("done"):
                if current.get("error"):
                    err = current["error"]
                    raise ApiError(
                        200,
                        err.get("code"),
                        str(err.get("message") or "operation failed"),
                        details=err.get("details"),
                        body=current,
                    )
                return current.get("response") or {}

            if time.time() >= deadline:
                raise ApiError(
                    0,
                    "operation_timeout",
                    f"Operation {operation_id} did not finish in time.",
                    body=current,
                )
            time.sleep(self.operation_poll_seconds)
            url = OPERATION_URL.format(operation_id=urllib.parse.quote(str(operation_id), safe=""))
            current = http_json("GET", url, body=None, token=self.token_provider.get())

    def create_cloud(
        self, organization_id: str, name: str, labels: Dict[str, str]
    ) -> Dict[str, Any]:
        body = {
            "organizationId": organization_id,
            "name": name,
            "description": "Created by yc_ip_hunter.",
            "labels": labels,
        }
        operation = self.request(
            "POST",
            f"{RESOURCE_MANAGER_URL}/clouds",
            body=body,
            operation_hint="cloud",
        )
        response = self.wait_operation(operation)
        if self.dry_run:
            response = {"id": "dry-cloud", "name": name, "labels": labels}
        if not response.get("id"):
            raise ApiError(200, "bad_response", "Cloud create response has no id", body=response)
        return response

    def list_clouds(self, organization_id: str) -> List[Dict[str, Any]]:
        clouds: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            query = {"organizationId": organization_id, "pageSize": "1000"}
            if page_token:
                query["pageToken"] = page_token
            url = f"{RESOURCE_MANAGER_URL}/clouds?{urllib.parse.urlencode(query)}"
            response = self.request("GET", url, body=None, operation_hint="cloud-list")
            clouds.extend(response.get("clouds") or [])
            page_token = str(response.get("nextPageToken") or "")
            if not page_token:
                return clouds

    def list_folders(self, cloud_id: str) -> List[Dict[str, Any]]:
        folders: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            query = {"cloudId": cloud_id, "pageSize": "1000"}
            if page_token:
                query["pageToken"] = page_token
            url = f"{RESOURCE_MANAGER_URL}/folders?{urllib.parse.urlencode(query)}"
            response = self.request("GET", url, body=None, operation_hint="folder-list")
            folders.extend(response.get("folders") or [])
            page_token = str(response.get("nextPageToken") or "")
            if not page_token:
                return folders

    def list_addresses(self, folder_id: str) -> List[Dict[str, Any]]:
        addresses: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            query = {"folderId": folder_id, "pageSize": "1000"}
            if page_token:
                query["pageToken"] = page_token
            url = f"{VPC_URL}/addresses?{urllib.parse.urlencode(query)}"
            response = self.request("GET", url, body=None, operation_hint="address-list")
            addresses.extend(response.get("addresses") or [])
            page_token = str(response.get("nextPageToken") or "")
            if not page_token:
                return addresses

    def get_cloud(self, cloud_id: str) -> Dict[str, Any]:
        quoted = urllib.parse.quote(cloud_id, safe="")
        return self.request(
            "GET",
            f"{RESOURCE_MANAGER_URL}/clouds/{quoted}",
            body=None,
            operation_hint="cloud-get",
        )

    def delete_cloud(self, cloud_id: str, immediate: bool, wait: bool = True) -> Optional[Dict[str, Any]]:
        quoted = urllib.parse.quote(cloud_id, safe="")
        url = f"{RESOURCE_MANAGER_URL}/clouds/{quoted}"
        if immediate:
            url = f"{url}?{urllib.parse.urlencode({'deleteAfter': IMMEDIATE_DELETE_AFTER})}"
        operation = self.request("DELETE", url, body=None, operation_hint="delete-cloud")
        if wait:
            self.wait_operation(operation)
        return operation

    def delete_address(self, address_id: str, wait: bool = True) -> Optional[Dict[str, Any]]:
        quoted = urllib.parse.quote(address_id, safe="")
        operation = self.request(
            "DELETE",
            f"{VPC_URL}/addresses/{quoted}",
            body=None,
            operation_hint="delete-address",
        )
        if wait:
            self.wait_operation(operation)
        return operation

    def bind_cloud_to_billing(self, billing_account_id: str, cloud_id: str) -> None:
        quoted = urllib.parse.quote(billing_account_id, safe="")
        body = {"billableObject": {"id": cloud_id, "type": "cloud"}}
        operation = self.request(
            "POST",
            f"{BILLING_URL}/billingAccounts/{quoted}/billableObjectBindings",
            body=body,
            operation_hint="billing-bind",
        )
        self.wait_operation(operation)

    def update_cloud_access_bindings(
        self, cloud_id: str, subject_id: str, role_id: str
    ) -> None:
        quoted = urllib.parse.quote(cloud_id, safe="")
        body = {
            "accessBindingDeltas": [
                {
                    "action": "ADD",
                    "accessBinding": {
                        "roleId": role_id,
                        "subject": {"id": subject_id, "type": "serviceAccount"},
                    },
                }
            ]
        }
        operation = self.request(
            "POST",
            f"{RESOURCE_MANAGER_URL}/clouds/{quoted}:updateAccessBindings",
            body=body,
            operation_hint="cloud-access",
        )
        self.wait_operation(operation)

    def update_folder_access_bindings(
        self, folder_id: str, subject_id: str, role_id: str
    ) -> None:
        quoted = urllib.parse.quote(folder_id, safe="")
        body = {
            "accessBindingDeltas": [
                {
                    "action": "ADD",
                    "accessBinding": {
                        "roleId": role_id,
                        "subject": {"id": subject_id, "type": "serviceAccount"},
                    },
                }
            ]
        }
        operation = self.request(
            "POST",
            f"{RESOURCE_MANAGER_URL}/folders/{quoted}:updateAccessBindings",
            body=body,
            operation_hint="folder-access",
        )
        self.wait_operation(operation)

    def delete_folder(
        self, folder_id: str, immediate: bool = True, wait: bool = True
    ) -> Optional[Dict[str, Any]]:
        quoted = urllib.parse.quote(folder_id, safe="")
        url = f"{RESOURCE_MANAGER_URL}/folders/{quoted}"
        if immediate:
            url = f"{url}?{urllib.parse.urlencode({'deleteAfter': IMMEDIATE_DELETE_AFTER})}"
        operation = self.request("DELETE", url, body=None, operation_hint="delete-folder")
        if wait:
            self.wait_operation(operation)
        return operation

    def create_folder(
        self, cloud_id: str, name: str, labels: Dict[str, str]
    ) -> Dict[str, Any]:
        body = {
            "cloudId": cloud_id,
            "name": name,
            "description": "Created by yc_ip_hunter.",
            "labels": labels,
        }
        operation = self.request(
            "POST",
            f"{RESOURCE_MANAGER_URL}/folders",
            body=body,
            operation_hint="folder",
        )
        response = self.wait_operation(operation)
        if self.dry_run:
            response = {"id": "dry-folder", "cloudId": cloud_id, "name": name, "labels": labels}
        if not response.get("id"):
            raise ApiError(200, "bad_response", "Folder create response has no id", body=response)
        return response

    def reserve_external_ipv4(
        self,
        folder_id: str,
        zone: str,
        name: str,
        labels: Dict[str, str],
        ip: Optional[str] = None,
    ) -> Dict[str, Any]:
        external_spec = {"zoneId": zone}
        if ip:
            external_spec["address"] = ip
        body = {
            "folderId": folder_id,
            "name": name,
            "description": "Reserved by yc_ip_hunter.",
            "labels": labels,
            "externalIpv4AddressSpec": external_spec,
            "deletionProtection": False,
        }
        operation = self.request(
            "POST",
            f"{VPC_URL}/addresses",
            body=body,
            operation_hint="address",
        )
        response = self.wait_operation(operation)
        if self.dry_run:
            dry_ip = ip or self.dry_run_allocated_ip
            response = {
                "id": "dry-address",
                "externalIpv4Address": {"address": dry_ip, "zoneId": zone},
            }
        if not response.get("id"):
            raise ApiError(200, "bad_response", "Address create response has no id", body=response)
        return response


def sanitize_resource_name(raw: str, prefix: str = "iphunt", max_len: int = 63) -> str:
    value = raw.lower()
    value = re.sub(r"[^a-z0-9-]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    if not value or not value[0].isalpha():
        value = f"{prefix}-{value}"
    value = value[:max_len].strip("-")
    if len(value) < 3:
        value = f"{prefix}-{value}"
    return value[:max_len].strip("-")


def usable_bounds(network: ipaddress.IPv4Network) -> Tuple[int, int, int]:
    total = network.num_addresses
    if total <= 0:
        return 0, -1, 0
    if total <= 2:
        return 0, total - 1, total
    return 1, total - 2, total - 2


def candidate_ips(
    explicit_ips: Iterable[str],
    cidrs: Iterable[str],
    max_count: int,
    seed: Optional[int],
) -> Iterator[str]:
    yielded = set()
    count = 0

    for raw_ip in explicit_ips:
        if count >= max_count:
            return
        ip_obj = ipaddress.ip_address(str(raw_ip).strip())
        if ip_obj.version != 4:
            raise ConfigError(f"Only IPv4 is supported: {raw_ip}")
        ip = str(ip_obj)
        if ip in yielded:
            continue
        yielded.add(ip)
        count += 1
        yield ip

    networks: List[Tuple[ipaddress.IPv4Network, int]] = []
    for raw_cidr in cidrs:
        network = ipaddress.ip_network(str(raw_cidr).strip(), strict=False)
        if network.version != 4:
            raise ConfigError(f"Only IPv4 CIDR is supported: {raw_cidr}")
        start, end, usable_count = usable_bounds(network)
        if start <= end and usable_count > 0:
            networks.append((network, usable_count))

    if not networks:
        return

    rng = random.Random(seed)
    total_weight = sum(weight for _, weight in networks)
    if total_weight <= 0:
        return

    misses = 0
    max_misses = max(1000, max_count * 10)
    while count < max_count and misses < max_misses:
        pick = rng.randrange(total_weight)
        cumulative = 0
        selected = networks[-1][0]
        for network, weight in networks:
            cumulative += weight
            if pick < cumulative:
                selected = network
                break
        start, _, usable_count = usable_bounds(selected)
        offset = start + rng.randrange(usable_count)
        ip = str(ipaddress.ip_address(int(selected.network_address) + offset))
        if ip in yielded:
            misses += 1
            continue
        yielded.add(ip)
        count += 1
        misses = 0
        yield ip


def build_target_networks(cidrs: Iterable[str]) -> List[ipaddress.IPv4Network]:
    networks = []
    for raw_cidr in cidrs:
        network = ipaddress.ip_network(str(raw_cidr).strip(), strict=False)
        if network.version != 4:
            raise ConfigError(f"Only IPv4 CIDR is supported: {raw_cidr}")
        networks.append(network)
    return networks


def ip_matches_targets(
    ip: str,
    explicit_ips: Iterable[str],
    target_networks: Iterable[ipaddress.IPv4Network],
) -> bool:
    ip_obj = ipaddress.ip_address(ip)
    explicit_set = {str(ipaddress.ip_address(str(item).strip())) for item in explicit_ips}
    if str(ip_obj) in explicit_set:
        return True
    return any(ip_obj in network for network in target_networks)


def dry_run_success_ip(config: Dict[str, Any]) -> str:
    target_ips = config.get("target_ips") or []
    if target_ips:
        return str(ipaddress.ip_address(str(target_ips[0]).strip()))

    target_cidrs = config.get("target_cidrs") or DEFAULT_TARGET_CIDRS
    networks = build_target_networks(target_cidrs)
    if not networks:
        return "203.0.113.10"
    network = networks[0]
    start, _, _ = usable_bounds(network)
    return str(ipaddress.ip_address(int(network.network_address) + start))


def classify_api_error(error: ApiError) -> str:
    text = error.text()
    rate_terms = [
        "rate exceeded",
        "too many requests",
        "externaladdressescreation.rate",
        "rate limit",
    ]
    quota_terms = [
        "quota",
        "resource_exhausted",
        "resource exhausted",
        "limit exceeded",
        "limits exceeded",
        "exceeded limit",
        "too many",
    ]
    unavailable_terms = [
        "already",
        "not available",
        "unavailable",
        "allocated",
        "reserved",
        "occupied",
        "in use",
        "conflict",
        "address is not",
        "cannot allocate",
    ]
    transient_terms = [
        "timeout",
        "temporarily",
        "try again",
        "unavailable",
        "network_error",
    ]

    if any(term in text for term in rate_terms):
        return "rate_limit"
    if any(term in text for term in quota_terms):
        return "quota"
    if error.status == 429:
        return "rate_limit"
    if error.status in {500, 502, 503, 504, 0}:
        return "transient"
    if error.status == 409 or any(term in text for term in unavailable_terms):
        return "unavailable"
    if any(term in text for term in transient_terms):
        return "transient"
    return "fatal"


class IpHunter:
    def __init__(
        self,
        config: Dict[str, Any],
        config_path: Path,
        dry_run_override: Optional[bool],
        yes_delete_cloud: bool,
    ) -> None:
        self.config = config
        self.config_path = config_path
        self.config_dir = config_path.parent.resolve()
        self.dry_run = bool(config.get("dry_run", True))
        if dry_run_override is not None:
            self.dry_run = dry_run_override
        self.yes_delete_cloud = yes_delete_cloud
        self._protected_clouds: Set[str] = set()

        self.state_path = resolve_path(
            self.config_dir, str(config.get("state_file") or "state.json")
        )
        assert self.state_path is not None
        self.state = load_state(self.state_path)

        log_file = resolve_path(self.config_dir, config.get("log_file") or "run.log")
        setup_logging(log_file, bool(config.get("verbose", False)))

        self.token_provider = TokenProvider(config, self.config_dir)
        self.client = YandexCloudClient(
            token_provider=self.token_provider,
            dry_run=self.dry_run,
            operation_timeout_seconds=int(config.get("operation_timeout_seconds", 900)),
            operation_poll_seconds=float(config.get("operation_poll_seconds", 5)),
            dry_run_allocated_ip=dry_run_success_ip(config),
        )

        self.max_state_attempts = int(config.get("max_state_attempts", 20000))
        self._validate_config()

    def _validate_config(self) -> None:
        rotation_mode = str(self.config.get("rotation_mode") or "legacy").lower()
        if rotation_mode not in {"legacy", "folder", "cloud", "hybrid"}:
            raise ConfigError("rotation_mode must be 'folder', 'cloud', 'hybrid', or 'legacy'.")
        self.config["rotation_mode"] = rotation_mode
        if rotation_mode in {"legacy", "cloud", "hybrid"} and not self.config.get("organization_id"):
            raise ConfigError("organization_id is required.")
        if rotation_mode in {"legacy", "cloud", "hybrid"} and not self.config.get("billing_account_id"):
            raise ConfigError("billing_account_id is required.")
        if rotation_mode == "folder" and not (
            self.config.get("target_cloud_id") or self.config.get("cloud_id")
        ):
            raise ConfigError("target_cloud_id or cloud_id is required for folder mode.")
        zones = self.config.get("zones") or []
        if not zones and self.config.get("zone"):
            zones = [self.config["zone"]]
            self.config["zones"] = zones
        if not isinstance(zones, list) or not zones:
            raise ConfigError("zones must be a non-empty list.")
        if not self.config.get("target_ips") and not self.config.get("target_cidrs"):
            self.config["target_cidrs"] = DEFAULT_TARGET_CIDRS
        if int(self.config.get("max_ip_candidates_per_cloud", 1000)) <= 0:
            raise ConfigError("max_ip_candidates_per_cloud must be positive.")
        if int(self.config.get("max_cloud_recreations", 0)) < 0:
            raise ConfigError("max_cloud_recreations cannot be negative.")

    def run(self) -> int:
        if self.dry_run:
            LOGGER.warning("dry_run=true: no Yandex Cloud resources will be changed.")
        self.log_target_ranges()

        rotation_mode = str(self.config.get("rotation_mode") or "legacy").lower()
        if rotation_mode == "folder":
            return self.run_folder_rotation()
        if rotation_mode == "hybrid":
            return self.run_hybrid_rotation()
        if rotation_mode == "cloud":
            return self.run_cloud_rotation()

        cloud_id, folder_id = self.ensure_cloud_and_folder()
        max_recreations = int(self.config.get("max_cloud_recreations", 0))

        while True:
            LOGGER.info("Trying candidates in cloud=%s folder=%s", cloud_id, folder_id)
            try:
                result = self.try_current_cloud(cloud_id, folder_id)
            except QuotaHit as exc:
                LOGGER.warning("Quota or limit hit: %s", exc)
                result = None

            if result:
                self.state["success"] = dataclasses.asdict(result)
                self.persist_state()
                if result.dry_run:
                    LOGGER.info(
                        "[dry-run] Would reserve IP %s in zone %s.",
                        result.ip,
                        result.zone,
                    )
                else:
                    LOGGER.info(
                        "Reserved IP %s in zone %s as address %s.",
                        result.ip,
                        result.zone,
                        result.address_id,
                    )
                return 0

            done = int(self.state.get("cloud_recreations_done", 0))
            if done >= max_recreations:
                LOGGER.error(
                    "No requested IP reserved, and max_cloud_recreations=%s is exhausted.",
                    max_recreations,
                )
                self.persist_state()
                return 2

            cloud_id, folder_id = self.recreate_cloud(cloud_id)

    def run_folder_rotation(self) -> int:
        target_cloud_id = str(self.config.get("target_cloud_id") or self.config.get("cloud_id"))
        max_iterations = int(self.config.get("max_iterations", 1))
        base_backoff = float(self.config.get("cooldown_seconds", 15))
        max_backoff = float(self.config.get("backoff_max_seconds", 120))
        backoff = base_backoff

        for iteration in self.iteration_numbers(max_iterations):
            folder_id = ""
            folder_name = self.roll_name("roll", iteration)
            LOGGER.info(
                "Folder rotation iteration %s/%s in cloud %s.",
                iteration,
                self.iteration_limit_label(max_iterations),
                target_cloud_id,
            )
            try:
                folder_id = self.create_named_folder(target_cloud_id, folder_name)
                self.grant_self_access_to_folder(folder_id)
                self.sleep_after_iam_grants()
                result = self.allocate_and_classify(target_cloud_id, folder_id, iteration)
            except RateLimitHit as exc:
                LOGGER.warning("Rate limit hit: %s", exc)
                address_id = str(self.state.get("last_address_id") or "")
                if address_id:
                    self.submit_address_delete(address_id)
                if folder_id:
                    self.submit_folder_delete(folder_id)
                self.sleep_backoff(backoff)
                backoff = min(max_backoff, backoff * 2)
                continue
            except ApiError as exc:
                if folder_id:
                    self.submit_folder_delete(folder_id)
                raise self.step_error("folder rotation", exc) from exc

            if result:
                self.save_success(result)
                LOGGER.info(
                    "Reserved target IP %s in cloud=%s folder=%s address=%s.",
                    result.ip,
                    result.cloud_id,
                    result.folder_id,
                    result.address_id,
                )
                return 0

            address_id = str(self.state.get("last_address_id") or "")
            if address_id:
                self.submit_address_delete(address_id)
            self.submit_folder_delete(folder_id)
            self.sleep_backoff(float(self.config.get("iteration_sleep_seconds", 10)))
            backoff = base_backoff

        LOGGER.error("No requested IP reserved after max_iterations=%s.", max_iterations)
        return 2

    def run_cloud_rotation(self) -> int:
        max_iterations = int(self.config.get("max_iterations", 1))
        base_backoff = float(self.config.get("cooldown_seconds", 15))
        max_backoff = float(self.config.get("backoff_max_seconds", 240))
        backoff = base_backoff

        for iteration in self.iteration_numbers(max_iterations):
            LOGGER.info("Cloud rotation iteration %s/%s.", iteration, self.iteration_limit_label(max_iterations))
            if not self.wait_for_cloud_slot():
                if bool(self.config.get("continuous", False)):
                    continue
                return 2

            cloud_id = ""
            folder_id = ""
            address_id = ""
            try:
                cloud_id, folder_id = self.create_cloud_cycle(iteration)
                result = self.allocate_cloud_batch(cloud_id, folder_id, iteration)
                if result:
                    self.save_success(result)
                    LOGGER.info(
                        "Reserved target IP %s in cloud=%s folder=%s address=%s.",
                        result.ip,
                        result.cloud_id,
                        result.folder_id,
                        result.address_id,
                    )
                    return 0

                self.submit_cloud_delete(cloud_id)
                self.sleep_backoff(float(self.config.get("cloud_iteration_sleep_seconds", 45)))
                backoff = base_backoff
            except RateLimitHit as exc:
                LOGGER.warning("Rate limit hit: %s", exc)
                if address_id:
                    self.submit_address_delete(address_id)
                if cloud_id:
                    self.submit_cloud_delete(cloud_id)
                self.sleep_backoff(backoff)
                backoff = min(max_backoff, backoff * 2)
            except ApiError as exc:
                kind = classify_api_error(exc)
                if kind == "quota":
                    LOGGER.error(
                        "Cloud/API quota hit during cloud rotation: %s. "
                        "Wait for deleting clouds to disappear or lower max_parallel_clouds.",
                        exc.message,
                    )
                    return 2
                raise self.step_error("cloud rotation", exc) from exc

        LOGGER.error("No requested IP reserved after max_iterations=%s.", max_iterations)
        return 2

    def run_hybrid_rotation(self) -> int:
        max_iterations = int(self.config.get("max_iterations", 0))
        base_backoff = float(self.config.get("cooldown_seconds", 15))
        max_backoff = float(self.config.get("backoff_max_seconds", 240))
        backoff = base_backoff

        if bool(self.config.get("startup_scan_on_start", True)):
            existing_cloud_ids = self.startup_scan()
        else:
            existing_cloud_ids = []

        # Queue of pre-existing clouds to work through before creating new ones.
        # Protected clouds (have target IPs) are included so we can keep hunting
        # in them; submit_cloud_delete will refuse to delete them if quota is hit.
        existing_queue: Deque[str] = deque(existing_cloud_ids)
        deferred = 0

        for iteration in self.iteration_numbers(max_iterations):
            LOGGER.info(
                "Hybrid rotation cloud generation %s/%s.",
                iteration,
                self.iteration_limit_label(max_iterations),
            )
            cloud_id = ""
            folder_id = ""
            managed_cloud = False
            try:
                if existing_queue:
                    cloud_id = existing_queue.popleft()
                    LOGGER.info("Hybrid mode reusing existing cloud %s.", cloud_id)
                    try:
                        folder_id = self.create_folder_in_cloud(cloud_id, iteration)
                        deferred = 0
                    except ApiError as exc:
                        if "currently being deleted" in exc.text() or "scheduled for deletion" in exc.text():
                            if deferred >= len(existing_queue):
                                self.sleep_backoff(float(self.config.get("cloud_iteration_sleep_seconds", 45)))
                            deferred += 1
                            LOGGER.info("Cloud %s has pending folder deletions; trying next cloud.", cloud_id)
                            existing_queue.append(cloud_id)
                            continue
                        if exc.status == 403:
                            LOGGER.warning("No permission on existing cloud %s (SA role grant may have been interrupted); skipping.", cloud_id)
                            continue
                        raise
                    managed_cloud = cloud_id not in self._protected_clouds
                else:
                    if not self.wait_for_cloud_slot():
                        if bool(self.config.get("continuous", False)):
                            continue
                        return 2
                    cloud_id, folder_id = self.create_cloud_cycle(iteration)
                    managed_cloud = True
                result = self.run_address_rotation_in_cloud(cloud_id, folder_id, iteration)
                if result:
                    self.save_success(result)
                    LOGGER.info(
                        "Reserved target IP %s in cloud=%s folder=%s address=%s.",
                        result.ip,
                        result.cloud_id,
                        result.folder_id,
                        result.address_id,
                    )
                    if not bool(self.config.get("continuous", False)):
                        return 0
                    self._protected_clouds.add(cloud_id)
                    LOGGER.info("Continuous mode: keeping cloud %s and hunting for next target IP.", cloud_id)
                    backoff = base_backoff
                    continue

                LOGGER.warning(
                    "Address rotation in cloud %s hit a limit; rotating cloud.",
                    cloud_id,
                )
                if managed_cloud:
                    self.submit_cloud_delete(cloud_id)
                self.sleep_backoff(float(self.config.get("cloud_iteration_sleep_seconds", 45)))
                backoff = base_backoff
            except RateLimitHit as exc:
                LOGGER.warning("Rate limit hit during hybrid rotation: %s", exc)
                if cloud_id and managed_cloud:
                    self.submit_cloud_delete(cloud_id)
                self.sleep_backoff(backoff)
                backoff = min(max_backoff, backoff * 2)
            except QuotaHit as exc:
                LOGGER.warning("Quota hit during hybrid rotation: %s", exc)
                if cloud_id and managed_cloud:
                    self.submit_cloud_delete(cloud_id)
                self.sleep_backoff(backoff)
                backoff = min(max_backoff, backoff * 2)
            except ApiError as exc:
                kind = classify_api_error(exc)
                if kind == "quota":
                    LOGGER.warning(
                        "Cloud/API quota hit during hybrid rotation: %s. "
                        "Waiting before the next cloud generation.",
                        exc.message,
                    )
                    self.sleep_backoff(float(self.config.get("cloud_quota_wait_seconds", 120)))
                    backoff = base_backoff
                    continue
                raise self.step_error("hybrid rotation", exc) from exc

        LOGGER.error("No requested IP reserved after max_iterations=%s.", max_iterations)
        return 2

    def ensure_hybrid_address_scope(self, iteration: int) -> Tuple[str, str]:
        use_service_cloud = bool(self.config.get("hybrid_use_service_cloud_first", False))
        cloud_id = str(
            self.config.get("target_cloud_id")
            or self.config.get("cloud_id")
            or (self.config.get("service_cloud_id") if use_service_cloud else "")
            or ""
        )
        if not cloud_id:
            LOGGER.info("Hybrid mode starts by creating a disposable hunting cloud.")
            if not self.wait_for_cloud_slot():
                if bool(self.config.get("continuous", False)):
                    while not self.wait_for_cloud_slot():
                        LOGGER.info("Cloud slot still full; retrying wait.")
                else:
                    raise QuotaHit("No cloud slot is available for hybrid mode.")

            return self.create_cloud_cycle(iteration)

        explicit_folder_id = str(self.config.get("folder_id") or "")
        state_hybrid_cloud_id = str(self.state.get("hybrid_cloud_id") or "")
        state_current_cloud_id = str(self.state.get("current_cloud_id") or "")
        folder_id = explicit_folder_id
        if not folder_id and state_hybrid_cloud_id == cloud_id:
            folder_id = str(self.state.get("hybrid_folder_id") or "")
        if not folder_id and state_current_cloud_id == cloud_id:
            folder_id = str(self.state.get("current_folder_id") or "")
        if folder_id:
            LOGGER.info(
                "Hybrid mode starts with existing cloud=%s folder=%s.",
                cloud_id,
                folder_id,
            )
            return cloud_id, folder_id
        if self.state.get("hybrid_folder_id") or self.state.get("current_folder_id"):
            LOGGER.warning(
                "Ignoring stale folder from state because it belongs to another cloud."
            )

        LOGGER.info(
            "Hybrid mode starts with existing cloud=%s; creating one working folder.",
            cloud_id,
        )
        self.grant_self_access_to_cloud(cloud_id)
        folder_id = self.create_named_folder(cloud_id, self.roll_name("folder", iteration))
        self.grant_self_access_to_folder(folder_id)
        self.sleep_after_iam_grants()
        self.state["hybrid_cloud_id"] = cloud_id
        self.state["hybrid_folder_id"] = folder_id
        self.persist_state()
        return cloud_id, folder_id

    def can_delete_hybrid_cloud(self, cloud_id: str) -> bool:
        service_cloud_id = str(self.config.get("service_cloud_id") or "")
        if service_cloud_id and cloud_id == service_cloud_id:
            return False
        return True

    def run_address_rotation_in_cloud(
        self, cloud_id: str, folder_id: str, cloud_iteration: int
    ) -> Optional[AttemptResult]:
        max_attempts = int(self.config.get("hybrid_max_address_attempts_per_cloud", 0))
        delay = float(self.config.get("address_iteration_sleep_seconds", 2))
        base_backoff = float(self.config.get("cooldown_seconds", 15))
        max_backoff = float(self.config.get("backoff_max_seconds", 240))
        max_consecutive_limits = int(self.config.get("hybrid_address_limit_rotates_cloud_after", 0))
        backoff = base_backoff
        consecutive_limits = 0

        for address_iteration in self.iteration_numbers(max_attempts):
            LOGGER.info(
                "Address rotation in cloud %s attempt %s/%s.",
                cloud_id,
                address_iteration,
                self.iteration_limit_label(max_attempts),
            )
            try:
                result = self.allocate_and_classify(
                    cloud_id,
                    folder_id,
                    cloud_iteration,
                    address_iteration,
                )
            except RateLimitHit as exc:
                consecutive_limits += 1
                LOGGER.warning(
                    "Address creation rate limit in cloud %s (%s/%s): %s",
                    cloud_id,
                    consecutive_limits,
                    self.iteration_limit_label(max_consecutive_limits),
                    exc,
                )
                if max_consecutive_limits and consecutive_limits >= max_consecutive_limits:
                    LOGGER.warning(
                        "Address rate limit persisted for %s consecutive attempts; rotating cloud.",
                        consecutive_limits,
                    )
                    return None
                self.sleep_backoff(backoff)
                backoff = min(max_backoff, backoff * 2)
                continue
            except QuotaHit as exc:
                consecutive_limits += 1
                LOGGER.warning(
                    "Address quota hit in cloud %s (%s/%s): %s",
                    cloud_id,
                    consecutive_limits,
                    self.iteration_limit_label(max_consecutive_limits),
                    exc,
                )
                if max_consecutive_limits and consecutive_limits >= max_consecutive_limits:
                    LOGGER.warning(
                        "Address quota persisted for %s consecutive attempts; rotating cloud.",
                        consecutive_limits,
                    )
                    return None
                self.sleep_backoff(backoff)
                backoff = min(max_backoff, backoff * 2)
                continue

            if result:
                return result

            consecutive_limits = 0
            backoff = base_backoff
            address_id = str(self.state.get("last_address_id") or "")
            if address_id:
                self.submit_address_delete(address_id)
            self.sleep_backoff(delay)

        LOGGER.warning(
            "Address attempt limit %s reached in cloud %s.",
            max_attempts,
            cloud_id,
        )
        return None

    def allocate_cloud_batch(
        self, cloud_id: str, folder_id: str, iteration: int
    ) -> Optional[AttemptResult]:
        max_addresses = int(self.config.get("max_addresses_per_cloud", 9))
        if max_addresses <= 0:
            raise ConfigError("max_addresses_per_cloud must be positive.")
        delay = float(self.config.get("address_iteration_sleep_seconds", 7))

        for address_index in range(1, max_addresses + 1):
            LOGGER.info(
                "Cloud %s address attempt %s/%s.",
                cloud_id,
                address_index,
                max_addresses,
            )
            result = self.allocate_and_classify(cloud_id, folder_id, iteration, address_index)
            if result:
                return result

            address_id = str(self.state.get("last_address_id") or "")
            if address_id:
                self.submit_address_delete(address_id)
            if address_index < max_addresses:
                self.sleep_backoff(delay)

        LOGGER.info("All %s address attempts in cloud %s missed target ranges.", max_addresses, cloud_id)
        return None

    def iteration_numbers(self, max_iterations: int) -> Iterator[int]:
        if max_iterations == 0:
            iteration = 1
            while True:
                yield iteration
                iteration += 1
        yield from range(1, max_iterations + 1)

    def iteration_limit_label(self, max_iterations: int) -> str:
        return "until-success" if max_iterations == 0 else str(max_iterations)

    def roll_name(self, prefix: str, iteration: int) -> str:
        raw_prefix = str(self.config.get(f"{prefix}_name_prefix") or self.config.get("cloud_name_prefix") or prefix)
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
        return sanitize_resource_name(f"{raw_prefix}-{timestamp}-{iteration}")

    def primary_zone(self) -> str:
        return str(self.config.get("zone") or (self.config.get("zones") or ["ru-central1-a"])[0])

    def log_target_ranges(self) -> None:
        target_ips = self.config.get("target_ips") or []
        target_cidrs = self.config.get("target_cidrs") or DEFAULT_TARGET_CIDRS
        if target_ips:
            LOGGER.warning("Explicit target IPs are configured: %s", ", ".join(map(str, target_ips)))
        LOGGER.warning(
            "TARGET CIDRS: %s",
            ", ".join(str(cidr) for cidr in target_cidrs),
        )

    def sleep_backoff(self, seconds: float) -> None:
        if seconds <= 0:
            return
        if self.dry_run:
            LOGGER.info("[dry-run] Would sleep %.1f seconds.", seconds)
            return
        LOGGER.info("Sleeping %.1f seconds.", seconds)
        time.sleep(seconds)

    def sleep_after_iam_grants(self) -> None:
        self.sleep_backoff(float(self.config.get("iam_propagation_sleep_seconds", 15)))

    def create_named_folder(self, cloud_id: str, folder_name: str) -> str:
        labels = {"managed-by": "yc-ip-hunter", "purpose": "ip-hunt"}
        folder = self.client.create_folder(cloud_id, folder_name, labels)
        folder_id = str(folder["id"])
        LOGGER.info("Created folder %s (%s).", folder_name, folder_id)
        return folder_id

    def create_folder_in_cloud(self, cloud_id: str, iteration: int) -> str:
        """Create a fresh hunting folder inside an existing cloud and grant SA access."""
        folder_id = self.create_named_folder(cloud_id, self.roll_name("folder", iteration))
        self.grant_self_access_to_folder(folder_id)
        self.sleep_after_iam_grants()
        return folder_id

    def create_cloud_cycle(self, iteration: int) -> Tuple[str, str]:
        cloud_name = self.roll_name("cloud", iteration)
        labels = {"managed-by": "yc-ip-hunter", "purpose": "ip-hunt"}
        try:
            cloud = self.client.create_cloud(
                organization_id=str(self.config["organization_id"]),
                name=cloud_name,
                labels=labels,
            )
        except ApiError as exc:
            if classify_api_error(exc) == "quota":
                raise
            raise self.step_error("create_cloud", exc) from exc

        cloud_id = str(cloud["id"])
        LOGGER.info("Created cloud %s (%s).", cloud_name, cloud_id)
        self.state.setdefault("created_clouds", []).append(
            {"cloud_id": cloud_id, "name": cloud_name, "at": utc_now_rfc3339()}
        )
        self.persist_state()

        try:
            self.client.bind_cloud_to_billing(str(self.config["billing_account_id"]), cloud_id)
        except ApiError as exc:
            raise self.step_error("bind_billing", exc) from exc
        LOGGER.info("Bound cloud %s to billing account.", cloud_id)

        self.grant_self_access_to_cloud(cloud_id)
        folder_id = self.create_named_folder(cloud_id, self.roll_name("folder", iteration))
        self.grant_self_access_to_folder(folder_id)
        self.sleep_after_iam_grants()
        return cloud_id, folder_id

    def allocate_and_classify(
        self, cloud_id: str, folder_id: str, iteration: int, address_index: int = 1
    ) -> Optional[AttemptResult]:
        zone = self.primary_zone()
        LOGGER.info("Allocating random public IP in zone %s.", zone)
        retries = int(self.config.get("create_address_permission_retries", 3))
        retry_sleep = float(self.config.get("create_address_permission_retry_sleep_seconds", 10))
        address = None
        for attempt in range(1, retries + 1):
            try:
                address = self.client.reserve_external_ipv4(
                    folder_id=folder_id,
                    zone=zone,
                    name=sanitize_resource_name(f"iphunt-{iteration}-{address_index}-{int(time.time())}"),
                    labels={"managed-by": "yc-ip-hunter", "purpose": "ip-hunt"},
                )
                break
            except ApiError as exc:
                kind = classify_api_error(exc)
                if kind == "rate_limit":
                    raise RateLimitHit(exc.message) from exc
                if kind == "quota":
                    raise QuotaHit(exc.message) from exc
                if exc.status == 403 and attempt < retries:
                    LOGGER.warning(
                        "Permission denied during create_address; waiting for IAM propagation (%s/%s).",
                        attempt,
                        retries,
                    )
                    self.sleep_backoff(retry_sleep)
                    continue
                raise self.step_error("create_address", exc) from exc
        if address is None:
            raise ApiError(200, "bad_response", "Address create returned no response.")

        allocated_ip = (
            address.get("externalIpv4Address", {}).get("address")
            or address.get("external_ipv4_address", {}).get("address")
        )
        address_id = str(address.get("id") or "")
        if not allocated_ip or not address_id:
            raise ApiError(200, "bad_response", "Address response has no id or allocated IP.", body=address)

        self.state["last_address_id"] = address_id
        self.state["last_allocated_ip"] = allocated_ip
        self.track_cloud_address(cloud_id, address_id, str(allocated_ip))
        self.persist_state()

        target_networks = build_target_networks(self.config.get("target_cidrs") or DEFAULT_TARGET_CIDRS)
        if ip_matches_targets(str(allocated_ip), self.config.get("target_ips") or [], target_networks):
            LOGGER.warning(
                "TARGET MATCH: allocated IP %s is in configured target ranges. Stopping and keeping address %s.",
                allocated_ip,
                address_id,
            )
            return AttemptResult(
                ip=str(allocated_ip),
                zone=zone,
                address_id=address_id,
                cloud_id=cloud_id,
                folder_id=folder_id,
                dry_run=self.dry_run,
            )

        LOGGER.info("Allocated IP %s is not in target ranges.", allocated_ip)
        return None

    def save_success(self, result: AttemptResult) -> None:
        self.state["success"] = dataclasses.asdict(result)
        self.persist_state()
        self.notify_success(result)
        self.open_success_video()

    def open_success_video(self) -> bool:
        if not config_bool(self.config.get("open_success_video"), default=True):
            return False
        url = str(self.config.get("success_video_url") or SUCCESS_VIDEO_URL).strip()
        if not url:
            return False
        try:
            launch_url = build_success_video_launcher(url)
            opened = bool(webbrowser.open(launch_url, new=2, autoraise=True))
        except Exception as exc:
            LOGGER.debug("Success video failed to open: %s", exc)
            return False
        if opened:
            LOGGER.info("Opened success video.")
        else:
            LOGGER.debug("Success video was not opened by the local browser.")
        return opened

    def notify_success(self, result: AttemptResult) -> None:
        notifications = self.config.get("notifications") or {}
        if not isinstance(notifications, dict):
            return

        telegram = notifications.get("telegram") or {}
        if not isinstance(telegram, dict):
            return

        notifications_enabled = config_bool(notifications.get("enabled"), default=False)
        telegram_enabled = config_bool(
            telegram.get("enabled"),
            default=notifications_enabled,
        )
        if not telegram_enabled:
            return

        self.send_telegram_notification(result)

    def send_telegram_notification(self, result: AttemptResult) -> bool:
        notifications = self.config.get("notifications") or {}
        telegram = notifications.get("telegram") or {}
        chat_id = str(telegram.get("chat_id") or "").strip()
        token = str(telegram.get("bot_token") or "").strip()
        token_env = str(telegram.get("bot_token_env") or "TELEGRAM_BOT_TOKEN")
        if not token:
            token = str(os.getenv(token_env) or "").strip()
        if not token or not chat_id:
            LOGGER.warning(
                "Telegram notification is enabled, but bot token or chat_id is missing."
            )
            return False

        text = (
            "YC IP Hunter found a target IP\n\n"
            f"IP: {result.ip}\n"
            f"Zone: {result.zone}\n"
            f"Cloud: {result.cloud_id}\n"
            f"Folder: {result.folder_id}\n"
            f"Address: {result.address_id}"
        )
        url = f"https://api.telegram.org/bot{urllib.parse.quote(token, safe='')}/sendMessage"
        body = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        try:
            http_json("POST", url, body=body, token=None, timeout=20)
            LOGGER.info("Telegram notification sent to chat %s.", chat_id)
            return True
        except ApiError as exc:
            LOGGER.warning("Telegram notification failed: %s", exc)
            return False
        except Exception as exc:  # Notification must never break a reserved target IP.
            LOGGER.warning("Telegram notification failed: %s", exc)
            return False

    def test_telegram_notification(self) -> bool:
        return self.send_telegram_notification(
            AttemptResult(
                ip="203.0.113.10",
                zone=str((self.config.get("zones") or [self.config.get("zone") or "ru-central1-a"])[0]),
                address_id="telegram-test",
                cloud_id="telegram-test",
                folder_id="telegram-test",
                dry_run=True,
            )
        )

    def track_cloud_address(self, cloud_id: str, address_id: str, ip: str) -> None:
        addresses_by_cloud = self.state.setdefault("addresses_by_cloud", {})
        cloud_addresses = addresses_by_cloud.setdefault(cloud_id, [])
        if not any(item.get("address_id") == address_id for item in cloud_addresses):
            cloud_addresses.append(
                {
                    "address_id": address_id,
                    "ip": ip,
                    "at": utc_now_rfc3339(),
                    "delete_submitted": False,
                }
            )

    def mark_cloud_address_delete_submitted(self, address_id: str) -> None:
        for cloud_addresses in (self.state.get("addresses_by_cloud") or {}).values():
            for item in cloud_addresses:
                if item.get("address_id") == address_id:
                    item["delete_submitted"] = True
                    item["delete_submitted_at"] = utc_now_rfc3339()

    def submit_address_delete(self, address_id: str) -> None:
        if not address_id:
            return
        LOGGER.info("Submitting async delete for address %s.", address_id)
        try:
            self.client.delete_address(address_id, wait=False)
            self.mark_cloud_address_delete_submitted(address_id)
            self.persist_state()
        except ApiError as exc:
            if "not found" in exc.text() or exc.status == 404:
                self.mark_cloud_address_delete_submitted(address_id)
                self.persist_state()
                return
            raise

    def submit_folder_delete(self, folder_id: str) -> None:
        if not folder_id:
            return
        LOGGER.warning("Submitting async delete for folder %s.", folder_id)
        try:
            self.client.delete_folder(folder_id, immediate=True, wait=False)
        except ApiError as exc:
            text = exc.text()
            if "scheduled for deletion" in text or "currently being deleted" in text or exc.status == 404:
                LOGGER.info("Folder %s is already being deleted; skipping.", folder_id)
                return
            raise

    def submit_cloud_delete(self, cloud_id: str) -> None:
        if not cloud_id:
            return
        if cloud_id in self._protected_clouds:
            LOGGER.info("Cloud %s has target IPs; skipping deletion.", cloud_id)
            return
        self.ensure_managed_cloud_delete_allowed(cloud_id)
        self.cleanup_cloud_addresses(cloud_id)
        LOGGER.warning("Submitting async delete for cloud %s.", cloud_id)
        self.client.delete_cloud(cloud_id, immediate=True, wait=False)
        self.state.setdefault("deleting_clouds", []).append(
            {"cloud_id": cloud_id, "at": utc_now_rfc3339()}
        )
        self.persist_state()

    def cleanup_cloud_addresses(self, cloud_id: str) -> None:
        addresses_by_cloud = self.state.get("addresses_by_cloud") or {}
        cloud_addresses = list(addresses_by_cloud.get(cloud_id) or [])
        if not cloud_addresses:
            return

        retries = int(self.config.get("address_delete_retries", 3))
        delay = float(self.config.get("address_delete_retry_sleep_seconds", 2))
        cleanup_sleep = float(self.config.get("pre_cloud_delete_cleanup_sleep_seconds", 3))
        address_ids = []
        seen = set()
        for item in cloud_addresses:
            address_id = str(item.get("address_id") or "")
            if address_id and address_id not in seen:
                address_ids.append(address_id)
                seen.add(address_id)

        LOGGER.info(
            "Pre-delete cleanup: submitting delete for %s known addresses in cloud %s.",
            len(address_ids),
            cloud_id,
        )
        for address_id in address_ids:
            for attempt in range(1, retries + 1):
                try:
                    self.submit_address_delete(address_id)
                    break
                except ApiError as exc:
                    if attempt >= retries:
                        LOGGER.warning(
                            "Address %s delete failed after %s attempts: %s",
                            address_id,
                            retries,
                            exc.message,
                        )
                        break
                    LOGGER.warning(
                        "Address %s delete attempt %s/%s failed: %s",
                        address_id,
                        attempt,
                        retries,
                        exc.message,
                    )
                    self.sleep_backoff(delay)
        self.state.setdefault("pre_delete_cleanups", []).append(
            {
                "cloud_id": cloud_id,
                "address_count": len(address_ids),
                "at": utc_now_rfc3339(),
            }
        )
        self.persist_state()
        self.sleep_backoff(cleanup_sleep)

    def ensure_managed_cloud_delete_allowed(self, cloud_id: str) -> None:
        service_cloud_id = str(self.config.get("service_cloud_id") or "")
        if service_cloud_id and cloud_id == service_cloud_id:
            raise ConfigError(f"Refusing to delete service cloud {cloud_id}.")
        if not self.yes_delete_cloud and not self.dry_run:
            raise ConfigError("Refusing to delete cloud without --yes-delete-cloud.")

    def wait_for_cloud_slot(self) -> bool:
        max_parallel = int(self.config.get("max_parallel_clouds", 4))
        wait_seconds = float(self.config.get("cloud_quota_wait_seconds", 120))
        max_wait_cycles = int(self.config.get("cloud_quota_max_wait_cycles", 10))

        for cycle in range(max_wait_cycles + 1):
            try:
                count = self.count_non_service_clouds()
            except ApiError as exc:
                kind = classify_api_error(exc)
                if kind in {"transient", "rate_limit"}:
                    LOGGER.warning(
                        "Could not check organization cloud count (%s); retrying after wait.",
                        exc.message,
                    )
                    self.sleep_backoff(wait_seconds)
                    continue
                raise
            LOGGER.info("Organization cloud count for rotation: %s/%s.", count, max_parallel)
            if count < max_parallel:
                return True
            if cycle >= max_wait_cycles:
                LOGGER.error("Cloud quota gate is still full after %s wait cycles.", max_wait_cycles)
                return False
            self.sleep_backoff(wait_seconds)
        return False

    def count_non_service_clouds(self) -> int:
        organization_id = str(self.config.get("organization_id") or "")
        if not organization_id:
            return 0
        clouds = self.client.list_clouds(organization_id)
        service_cloud_id = str(self.config.get("service_cloud_id") or "")
        count = 0
        for cloud in clouds:
            cloud_id = str(cloud.get("id") or "")
            if service_cloud_id and cloud_id == service_cloud_id:
                continue
            count += 1
        return count

    def startup_scan(self) -> List[str]:
        """Scan all existing non-service clouds on startup.

        For each cloud/folder/address found:
        - If the address matches target_cidrs: mark cloud as protected, track the address.
        - If not: delete the address. If the folder becomes empty, delete the folder.

        Returns list of existing cloud IDs (in discovery order) for the rotation queue.
        """
        organization_id = str(self.config.get("organization_id") or "")
        if not organization_id:
            return []
        service_cloud_id = str(self.config.get("service_cloud_id") or "")
        target_ips = self.config.get("target_ips") or []
        target_networks = build_target_networks(self.config.get("target_cidrs") or DEFAULT_TARGET_CIDRS)

        try:
            all_clouds = self.client.list_clouds(organization_id)
        except ApiError as exc:
            LOGGER.warning("Startup scan: could not list clouds: %s. Skipping scan.", exc.message)
            return []

        cloud_ids: List[str] = []
        for cloud in all_clouds:
            cloud_id = str(cloud.get("id") or "")
            if not cloud_id:
                continue
            if service_cloud_id and cloud_id == service_cloud_id:
                continue
            cloud_ids.append(cloud_id)

        LOGGER.info("Startup scan: found %s non-service cloud(s) to inspect.", len(cloud_ids))

        for cloud_id in cloud_ids:
            try:
                folders = self.client.list_folders(cloud_id)
            except ApiError as exc:
                LOGGER.warning("Startup scan: could not list folders in cloud %s: %s. Skipping.", cloud_id, exc.message)
                continue

            for folder in folders:
                folder_id = str(folder.get("id") or "")
                if not folder_id:
                    continue
                try:
                    addresses = self.client.list_addresses(folder_id)
                except ApiError as exc:
                    LOGGER.warning("Startup scan: could not list addresses in folder %s: %s. Skipping.", folder_id, exc.message)
                    continue

                folder_has_target = False
                for addr in addresses:
                    address_id = str(addr.get("id") or "")
                    allocated = (
                        addr.get("externalIpv4Address", {}).get("address")
                        or addr.get("external_ipv4_address", {}).get("address")
                    )
                    if not address_id or not allocated:
                        continue
                    ip = str(allocated)
                    if ip_matches_targets(ip, target_ips, target_networks):
                        LOGGER.info(
                            "Startup scan: target IP %s found in cloud %s folder %s — protecting cloud.",
                            ip, cloud_id, folder_id,
                        )
                        self.track_cloud_address(cloud_id, address_id, ip)
                        self._protected_clouds.add(cloud_id)
                        folder_has_target = True
                        zone = str(
                            addr.get("externalIpv4Address", {}).get("zoneId")
                            or addr.get("external_ipv4_address", {}).get("zoneId")
                            or addr.get("external_ipv4_address", {}).get("zone_id")
                            or ""
                        )
                        self.notify_success(
                            AttemptResult(
                                ip=ip,
                                zone=zone,
                                address_id=address_id,
                                cloud_id=cloud_id,
                                folder_id=folder_id,
                                dry_run=self.dry_run,
                            )
                        )
                    else:
                        LOGGER.info("Startup scan: deleting non-target address %s (%s) in folder %s.", address_id, ip, folder_id)
                        self.submit_address_delete(address_id)

                if not folder_has_target:
                    LOGGER.info("Startup scan: deleting empty/non-target folder %s in cloud %s.", folder_id, cloud_id)
                    self.submit_folder_delete(folder_id)

        if self._protected_clouds:
            LOGGER.info("Startup scan complete. Protected clouds (have target IPs): %s.", list(self._protected_clouds))
        else:
            LOGGER.info("Startup scan complete. No target IPs found in existing clouds.")

        return cloud_ids

    def step_error(self, step: str, exc: ApiError) -> ConfigError:
        if exc.status == 403:
            return ConfigError(
                f"Permission denied during {step}. Check service account roles for this step."
            )
        return ConfigError(f"{step} failed: {exc}")

    def ensure_cloud_and_folder(self) -> Tuple[str, str]:
        cloud_id = (
            self.state.get("current_cloud_id")
            or self.config.get("cloud_id")
            or self.config.get("cloud_id_to_delete")
        )
        folder_id = self.state.get("current_folder_id") or self.config.get("folder_id")

        if not cloud_id:
            LOGGER.info("No cloud_id configured; creating a new cloud.")
            cloud_id, folder_id = self.create_cloud_folder_pair()
            return cloud_id, folder_id

        if not self.state.get("current_cloud_id"):
            try:
                self.client.get_cloud(str(cloud_id))
            except ApiError as exc:
                text = exc.text()
                if exc.status in {403, 404} or "deleted" in text or "deletion" in text:
                    LOGGER.warning(
                        "Configured cloud %s is not usable (%s); creating/reusing a replacement.",
                        cloud_id,
                        exc.message,
                    )
                    return self.create_cloud_folder_pair()
                raise

        if (
            bool(self.config.get("start_by_recreating_cloud", False))
            and not folder_id
            and bool(self.config.get("allow_delete_cloud", False))
        ):
            LOGGER.info(
                "start_by_recreating_cloud=true; deleting configured cloud %s first.",
                cloud_id,
            )
            return self.recreate_cloud(str(cloud_id))

        if not folder_id:
            LOGGER.info("No folder_id configured; creating a folder in cloud %s.", cloud_id)
            if bool(self.config.get("auto_grant_current_resources", True)):
                try:
                    self.grant_self_access_to_cloud(str(cloud_id))
                except ApiError as exc:
                    if exc.status == 403:
                        raise ConfigError(
                            f"Permission denied while granting access on existing cloud {cloud_id}. "
                            "Open this cloud in Yandex Cloud console and grant huntersa/admin, "
                            "or wait until old ip-hunter clouds finish deletion so the script can "
                            "create a fresh cloud."
                        ) from exc
                    raise
            folder_id = self.create_folder(cloud_id)

        if bool(self.config.get("auto_grant_current_resources", True)):
            self.grant_self_access_to_cloud(str(cloud_id))
            self.grant_self_access_to_folder(str(folder_id))
            self.sleep_after_iam_grants()

        self.state["current_cloud_id"] = cloud_id
        self.state["current_folder_id"] = folder_id
        self.persist_state()
        return str(cloud_id), str(folder_id)

    def create_cloud_folder_pair(self) -> Tuple[str, str]:
        name_prefix = sanitize_resource_name(str(self.config.get("cloud_name_prefix") or "ip-hunter"))
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
        cloud_name = sanitize_resource_name(f"{name_prefix}-{timestamp}")
        labels = {"managed-by": "yc-ip-hunter", "purpose": "ip-hunt"}

        try:
            cloud = self.client.create_cloud(
                organization_id=str(self.config["organization_id"]),
                name=cloud_name,
                labels=labels,
            )
        except ApiError as exc:
            if classify_api_error(exc) == "quota":
                fallback = self.find_reusable_cloud(name_prefix)
                if fallback:
                    cloud_id = str(fallback["id"])
                    LOGGER.warning(
                        "Cloud creation quota hit; reusing existing cloud %s (%s).",
                        fallback.get("name"),
                        cloud_id,
                    )
                    self.grant_self_access_to_cloud(cloud_id)
                    folder_id = self.create_folder(cloud_id)
                    self.grant_self_access_to_folder(folder_id)
                    self.state["current_cloud_id"] = cloud_id
                    self.state["current_folder_id"] = folder_id
                    self.persist_state()
                    return cloud_id, folder_id
            if exc.status == 403:
                raise ConfigError(
                    "Permission denied while creating a new cloud. Grant the "
                    "service account a role on the organization, for example "
                    "resource-manager.editor or admin, then run again."
                ) from exc
            raise
        cloud_id = str(cloud["id"])
        LOGGER.info("Created cloud %s (%s).", cloud_name, cloud_id)

        self.client.bind_cloud_to_billing(str(self.config["billing_account_id"]), cloud_id)
        LOGGER.info("Bound cloud %s to billing account.", cloud_id)

        self.grant_self_access_to_cloud(cloud_id)

        folder_id = self.create_folder(cloud_id)
        self.grant_self_access_to_folder(folder_id)
        self.sleep_after_iam_grants()
        self.state["current_cloud_id"] = cloud_id
        self.state["current_folder_id"] = folder_id
        self.persist_state()
        return cloud_id, folder_id

    def find_reusable_cloud(self, name_prefix: str) -> Optional[Dict[str, Any]]:
        clouds = self.client.list_clouds(str(self.config["organization_id"]))
        candidates = []
        for cloud in clouds:
            name = str(cloud.get("name") or "")
            labels = cloud.get("labels") or {}
            if name.startswith(name_prefix) or labels.get("managed-by") == "yc-ip-hunter":
                candidates.append(cloud)
        if not candidates:
            return None
        candidates.sort(key=lambda item: str(item.get("createdAt") or ""), reverse=True)
        return candidates[0]

    def grant_self_access_to_cloud(self, cloud_id: str) -> None:
        service_account_id = self.token_provider.service_account_id
        if not service_account_id:
            LOGGER.warning(
                "Cannot auto-grant cloud access: service account id is unknown."
            )
            return
        role_id = str(self.config.get("new_cloud_service_account_role") or "admin")
        LOGGER.info(
            "Granting %s to service account %s on cloud %s.",
            role_id,
            service_account_id,
            cloud_id,
        )
        self.client.update_cloud_access_bindings(cloud_id, str(service_account_id), role_id)
        LOGGER.info(
            "Granted %s to service account %s on cloud %s.",
            role_id,
            service_account_id,
            cloud_id,
        )

    def grant_self_access_to_folder(self, folder_id: str) -> None:
        service_account_id = self.token_provider.service_account_id
        if not service_account_id:
            LOGGER.warning(
                "Cannot auto-grant folder access: service account id is unknown."
            )
            return
        role_id = str(self.config.get("new_folder_service_account_role") or "admin")
        LOGGER.info(
            "Granting %s to service account %s on folder %s.",
            role_id,
            service_account_id,
            folder_id,
        )
        self.client.update_folder_access_bindings(folder_id, str(service_account_id), role_id)
        LOGGER.info(
            "Granted %s to service account %s on folder %s.",
            role_id,
            service_account_id,
            folder_id,
        )

    def create_folder(self, cloud_id: str) -> str:
        folder_name = sanitize_resource_name(str(self.config.get("folder_name") or "ip-hunter"))
        labels = {"managed-by": "yc-ip-hunter", "purpose": "ip-hunt"}
        folder = self.client.create_folder(cloud_id, folder_name, labels)
        folder_id = str(folder["id"])
        LOGGER.info("Created folder %s (%s).", folder_name, folder_id)
        return folder_id

    def recreate_cloud(self, old_cloud_id: str) -> Tuple[str, str]:
        if not bool(self.config.get("allow_delete_cloud", False)):
            raise ConfigError(
                "Refusing to delete cloud: set allow_delete_cloud=true in config."
            )
        if not self.yes_delete_cloud and not self.dry_run:
            raise ConfigError(
                "Refusing to delete cloud without --yes-delete-cloud."
            )

        immediate = bool(self.config.get("immediate_delete_cloud", True))
        LOGGER.warning("Deleting cloud %s (immediate=%s).", old_cloud_id, immediate)
        try:
            self.client.delete_cloud(str(old_cloud_id), immediate=immediate, wait=False)
            LOGGER.info(
                "Delete operation submitted for cloud %s; continuing without waiting.",
                old_cloud_id,
            )
        except ApiError as exc:
            text = exc.text()
            if "scheduled for deletion" in text or "currently being deleted" in text:
                LOGGER.warning(
                    "Cloud %s is already being deleted; creating a replacement.",
                    old_cloud_id,
                )
            else:
                raise

        done = int(self.state.get("cloud_recreations_done", 0)) + 1
        self.state["cloud_recreations_done"] = done
        self.state["last_deleted_cloud_id"] = old_cloud_id
        self.state.pop("current_cloud_id", None)
        self.state.pop("current_folder_id", None)
        self.persist_state()

        new_cloud_id, new_folder_id = self.create_cloud_folder_pair()
        LOGGER.info(
            "Recreated cloud generation %s: cloud=%s folder=%s.",
            done,
            new_cloud_id,
            new_folder_id,
        )
        return new_cloud_id, new_folder_id

    def try_current_cloud(self, cloud_id: str, folder_id: str) -> Optional[AttemptResult]:
        allocation_mode = str(self.config.get("allocation_mode") or "random").lower()
        if allocation_mode not in {"random", "specific"}:
            raise ConfigError("allocation_mode must be 'random' or 'specific'.")
        if allocation_mode == "specific":
            return self.try_specific_addresses(cloud_id, folder_id)
        return self.try_random_allocations(cloud_id, folder_id)

    def try_specific_addresses(
        self, cloud_id: str, folder_id: str
    ) -> Optional[AttemptResult]:
        target_ips = self.config.get("target_ips") or []
        target_cidrs = self.config.get("target_cidrs") or DEFAULT_TARGET_CIDRS
        max_candidates = int(self.config.get("max_ip_candidates_per_cloud", 1000))
        seed = self.config.get("random_seed")
        if seed is not None:
            seed = int(seed) + int(self.state.get("cloud_recreations_done", 0))

        zones = [str(zone) for zone in self.config["zones"]]
        attempted = set(self.state.get("attempted_keys") or [])
        delay = float(self.config.get("attempt_delay_seconds", 0.0))

        for ip in candidate_ips(target_ips, target_cidrs, max_candidates, seed):
            for zone in zones:
                key = f"{cloud_id}|{folder_id}|{zone}|{ip}"
                if key in attempted:
                    continue
                LOGGER.info("Trying IP %s in zone %s.", ip, zone)
                try:
                    address = self.client.reserve_external_ipv4(
                        folder_id=folder_id,
                        ip=ip,
                        zone=zone,
                        name=sanitize_resource_name(f"iphunt-{ip.replace('.', '-')}"),
                        labels={"managed-by": "yc-ip-hunter", "purpose": "ip-hunt"},
                    )
                    self.record_attempt(key, cloud_id, folder_id, zone, ip, "success", "")
                    return AttemptResult(
                        ip=ip,
                        zone=zone,
                        address_id=str(address["id"]),
                        cloud_id=cloud_id,
                        folder_id=folder_id,
                        dry_run=self.dry_run,
                    )
                except ApiError as exc:
                    kind = classify_api_error(exc)
                    self.record_attempt(key, cloud_id, folder_id, zone, ip, kind, exc.message)
                    if kind == "quota":
                        raise QuotaHit(exc.message) from exc
                    if kind == "fatal":
                        raise
                    LOGGER.info("Candidate %s/%s skipped: %s", ip, zone, exc.message)
                    if delay:
                        time.sleep(delay)
        LOGGER.info("Candidate limit exhausted for current cloud.")
        return None

    def try_random_allocations(
        self, cloud_id: str, folder_id: str
    ) -> Optional[AttemptResult]:
        target_ips = self.config.get("target_ips") or []
        target_cidrs = self.config.get("target_cidrs") or DEFAULT_TARGET_CIDRS
        target_networks = build_target_networks(target_cidrs)
        max_allocations = int(self.config.get("max_ip_candidates_per_cloud", 1000))
        zones = [str(zone) for zone in self.config["zones"]]
        delay = float(self.config.get("attempt_delay_seconds", 0.0))
        delete_misses = bool(self.config.get("delete_unmatched_addresses", True))

        allocation_number = int(self.state.get("random_allocations_done", 0))
        for _ in range(max_allocations):
            for zone in zones:
                allocation_number += 1
                name = sanitize_resource_name(f"iphunt-random-{allocation_number}")
                LOGGER.info("Allocating random public IP in zone %s.", zone)
                try:
                    address = self.client.reserve_external_ipv4(
                        folder_id=folder_id,
                        zone=zone,
                        name=name,
                        labels={"managed-by": "yc-ip-hunter", "purpose": "ip-hunt"},
                    )
                except ApiError as exc:
                    kind = classify_api_error(exc)
                    self.record_attempt(
                        f"{cloud_id}|{folder_id}|{zone}|random-{allocation_number}",
                        cloud_id,
                        folder_id,
                        zone,
                        "random",
                        kind,
                        exc.message,
                    )
                    if kind == "quota":
                        raise QuotaHit(exc.message) from exc
                    if kind == "fatal":
                        raise
                    LOGGER.info("Random allocation in %s skipped: %s", zone, exc.message)
                    continue

                allocated_ip = (
                    address.get("externalIpv4Address", {}).get("address")
                    or address.get("external_ipv4_address", {}).get("address")
                )
                address_id = str(address.get("id") or "")
                if not allocated_ip or not address_id:
                    raise ApiError(
                        200,
                        "bad_response",
                        "Address response has no id or allocated IP.",
                        body=address,
                    )

                status = "success" if ip_matches_targets(allocated_ip, target_ips, target_networks) else "miss"
                self.record_attempt(
                    f"{cloud_id}|{folder_id}|{zone}|{allocated_ip}",
                    cloud_id,
                    folder_id,
                    zone,
                    str(allocated_ip),
                    status,
                    "",
                )
                self.state["random_allocations_done"] = allocation_number
                self.persist_state()

                if status == "success":
                    return AttemptResult(
                        ip=str(allocated_ip),
                        zone=zone,
                        address_id=address_id,
                        cloud_id=cloud_id,
                        folder_id=folder_id,
                        dry_run=self.dry_run,
                    )

                LOGGER.info("Allocated IP %s is not in target ranges.", allocated_ip)
                if delete_misses:
                    LOGGER.info("Deleting unmatched address %s.", address_id)
                    self.client.delete_address(address_id)
                if delay:
                    time.sleep(delay)

        LOGGER.info("Random allocation limit exhausted for current cloud.")
        return None

    def record_attempt(
        self,
        key: str,
        cloud_id: str,
        folder_id: str,
        zone: str,
        ip: str,
        status: str,
        message: str,
    ) -> None:
        self.state.setdefault("attempted_keys", []).append(key)
        attempts = self.state.setdefault("attempts", [])
        attempts.append(
            {
                "at": utc_now_rfc3339(),
                "cloud_id": cloud_id,
                "folder_id": folder_id,
                "zone": zone,
                "ip": ip,
                "status": status,
                "message": message[:500],
            }
        )
        if len(attempts) > self.max_state_attempts:
            del attempts[: len(attempts) - self.max_state_attempts]
        if len(self.state["attempted_keys"]) > self.max_state_attempts:
            del self.state["attempted_keys"][
                : len(self.state["attempted_keys"]) - self.max_state_attempts
            ]
        self.persist_state()

    def persist_state(self) -> None:
        if self.dry_run:
            return
        save_json_atomic(self.state_path, self.state)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Try to reserve desired Yandex Cloud public IPv4 addresses."
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to JSON or YAML config. Default: config.json",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Force dry-run mode.")
    mode.add_argument("--run", action="store_true", help="Force live mode.")
    parser.add_argument(
        "--yes-delete-cloud",
        action="store_true",
        help="Required in live mode before the script may delete a cloud.",
    )
    parser.add_argument(
        "--test-telegram",
        action="store_true",
        help="Send one Telegram test message and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config).resolve()
    try:
        config = load_json_or_yaml(config_path)
        dry_run_override = True if args.dry_run else False if args.run else None
        hunter = IpHunter(
            config=config,
            config_path=config_path,
            dry_run_override=dry_run_override,
            yes_delete_cloud=bool(args.yes_delete_cloud),
        )
        if args.test_telegram:
            return 0 if hunter.test_telegram_notification() else 1
        return hunter.run()
    except KeyboardInterrupt:
        LOGGER.error("Interrupted.")
        return 130
    except (ConfigError, ApiError) as exc:
        if not LOGGER.handlers:
            logging.basicConfig(level=logging.INFO)
        LOGGER.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
