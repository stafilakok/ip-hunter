import importlib.util
import ipaddress
import sys
import unittest
import urllib.parse
import urllib.request
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("yc_ip_hunter.py")
SPEC = importlib.util.spec_from_file_location("yc_ip_hunter_module", MODULE_PATH)
yc = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = yc
SPEC.loader.exec_module(yc)


class CandidateIpTests(unittest.TestCase):
    def test_explicit_ips_are_first_and_deduplicated(self):
        got = list(
            yc.candidate_ips(
                explicit_ips=["192.0.2.10", "192.0.2.10", "192.0.2.11"],
                cidrs=[],
                max_count=10,
                seed=1,
            )
        )
        self.assertEqual(got, ["192.0.2.10", "192.0.2.11"])

    def test_cidr_candidates_stay_inside_network(self):
        got = list(
            yc.candidate_ips(
                explicit_ips=[],
                cidrs=["192.0.2.0/29"],
                max_count=6,
                seed=42,
            )
        )
        network = ipaddress.ip_network("192.0.2.0/29")
        self.assertEqual(len(got), 6)
        for ip in got:
            self.assertIn(ipaddress.ip_address(ip), network)
            self.assertNotEqual(ip, "192.0.2.0")
            self.assertNotEqual(ip, "192.0.2.7")

    def test_prefix_32_is_supported(self):
        got = list(
            yc.candidate_ips(
                explicit_ips=[],
                cidrs=["203.0.113.10/32"],
                max_count=1,
                seed=1,
            )
        )
        self.assertEqual(got, ["203.0.113.10"])

    def test_ip_matches_targets(self):
        networks = yc.build_target_networks(["198.51.100.0/24"])
        self.assertTrue(yc.ip_matches_targets("198.51.100.20", [], networks))
        self.assertTrue(yc.ip_matches_targets("203.0.113.7", ["203.0.113.7"], networks))
        self.assertFalse(yc.ip_matches_targets("203.0.113.8", [], networks))

    def test_yandex_51_250_range_matches_any_third_octet(self):
        networks = yc.build_target_networks(["51.250.0.0/16"])
        self.assertTrue(yc.ip_matches_targets("51.250.77.87", [], networks))
        self.assertTrue(yc.ip_matches_targets("51.250.92.205", [], networks))
        self.assertTrue(yc.ip_matches_targets("51.250.34.175", [], networks))

    def test_current_target_ranges_match_known_good_subnets(self):
        networks = yc.build_target_networks(
            [
                "51.250.0.0/16",
                "84.201.0.0/16",
                "95.161.0.0/16",
                "178.154.0.0/16",
                "130.193.0.0/16",
            ]
        )
        for ip in [
            "51.250.77.87",
            "84.201.188.1",
            "95.161.10.10",
            "178.154.20.20",
            "130.193.30.30",
        ]:
            self.assertTrue(yc.ip_matches_targets(ip, [], networks), ip)

    def test_dry_run_success_ip_uses_targets(self):
        got = yc.dry_run_success_ip({"target_cidrs": ["198.51.100.0/24"]})
        self.assertEqual(got, "198.51.100.1")


class ClassificationTests(unittest.TestCase):
    def test_quota_error(self):
        err = yc.ApiError(400, "RESOURCE_EXHAUSTED", "Quota limit exceeded")
        self.assertEqual(yc.classify_api_error(err), "quota")

    def test_unavailable_error(self):
        err = yc.ApiError(409, "ALREADY_EXISTS", "address already reserved")
        self.assertEqual(yc.classify_api_error(err), "unavailable")

    def test_fatal_error(self):
        err = yc.ApiError(403, "PERMISSION_DENIED", "permission denied")
        self.assertEqual(yc.classify_api_error(err), "fatal")

    def test_http_503_is_transient(self):
        err = yc.ApiError(503, "UNAVAILABLE", "service unavailable")
        self.assertEqual(yc.classify_api_error(err), "transient")

    def test_vpc_rate_limit_is_rate_limit(self):
        err = yc.ApiError(429, 8, "Quota limit vpc.externalAddressesCreation.rate exceeded")
        self.assertEqual(yc.classify_api_error(err), "rate_limit")

    def test_cloud_creation_quota_is_quota(self):
        err = yc.ApiError(429, 8, "Cloud creation quota exceeded")
        self.assertEqual(yc.classify_api_error(err), "quota")

    def test_scheduled_for_deletion_text_is_visible(self):
        err = yc.ApiError(400, 9, "Cloud 'abc' is scheduled for deletion")
        self.assertIn("scheduled for deletion", err.text())


class NameTests(unittest.TestCase):
    def test_resource_name_starts_with_letter(self):
        self.assertEqual(yc.sanitize_resource_name("1.2.3.4"), "iphunt-1-2-3-4")

    def test_resource_name_strips_bad_chars(self):
        self.assertEqual(yc.sanitize_resource_name("IP Hunt_One"), "ip-hunt-one")

    def test_immediate_delete_after_is_in_past(self):
        self.assertEqual(yc.IMMEDIATE_DELETE_AFTER, "1970-01-01T00:00:00Z")

    def test_delete_cloud_supports_non_waiting_mode(self):
        self.assertIn("wait: bool = True", MODULE_PATH.read_text(encoding="utf-8"))

    def test_delete_folder_supports_non_waiting_mode(self):
        self.assertIn("def delete_folder(", MODULE_PATH.read_text(encoding="utf-8"))


class IterationTests(unittest.TestCase):
    def test_zero_max_iterations_means_unbounded(self):
        hunter = object.__new__(yc.IpHunter)
        got = []
        for value in hunter.iteration_numbers(0):
            got.append(value)
            if len(got) == 3:
                break
        self.assertEqual(got, [1, 2, 3])

    def test_iteration_limit_label(self):
        hunter = object.__new__(yc.IpHunter)
        self.assertEqual(hunter.iteration_limit_label(0), "until-success")
        self.assertEqual(hunter.iteration_limit_label(5), "5")

    def test_cloud_batch_setting_exists(self):
        text = MODULE_PATH.read_text(encoding="utf-8")
        self.assertIn("max_addresses_per_cloud", text)
        self.assertIn("address_iteration_sleep_seconds", text)

    def test_hybrid_mode_setting_exists(self):
        text = MODULE_PATH.read_text(encoding="utf-8")
        self.assertIn("run_hybrid_rotation", text)
        self.assertIn("hybrid_max_address_attempts_per_cloud", text)
        self.assertIn("hybrid_address_limit_rotates_cloud_after", text)
        self.assertIn("hybrid_use_service_cloud_first", text)

    def test_validate_accepts_hybrid_mode(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {
            "rotation_mode": "hybrid",
            "organization_id": "org",
            "billing_account_id": "billing",
            "zones": ["ru-central1-a"],
            "target_cidrs": ["198.51.100.0/24"],
            "max_ip_candidates_per_cloud": 1,
            "max_cloud_recreations": 0,
        }
        hunter._validate_config()
        self.assertEqual(hunter.config["rotation_mode"], "hybrid")

    def test_hybrid_scope_ignores_stale_state_folder(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {
            "service_cloud_id": "cloud-new",
            "hybrid_use_service_cloud_first": True,
            "folder_name_prefix": "roll",
            "cloud_name_prefix": "ip-hunter",
            "new_cloud_service_account_role": "admin",
            "new_folder_service_account_role": "admin",
        }
        hunter.state = {
            "hybrid_cloud_id": "cloud-old",
            "hybrid_folder_id": "folder-old",
        }
        hunter.persist_state = lambda: None
        hunter.grant_self_access_to_cloud = lambda cloud_id: None
        hunter.grant_self_access_to_folder = lambda folder_id: None
        hunter.sleep_after_iam_grants = lambda: None
        hunter.create_named_folder = lambda cloud_id, folder_name: "folder-new"

        cloud_id, folder_id = hunter.ensure_hybrid_address_scope(1)

        self.assertEqual(cloud_id, "cloud-new")
        self.assertEqual(folder_id, "folder-new")
        self.assertEqual(hunter.state["hybrid_cloud_id"], "cloud-new")
        self.assertEqual(hunter.state["hybrid_folder_id"], "folder-new")

    def test_hybrid_scope_creates_cloud_instead_of_using_service_by_default(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {
            "service_cloud_id": "service-cloud",
            "hybrid_use_service_cloud_first": False,
        }
        hunter.state = {}
        hunter.wait_for_cloud_slot = lambda: True
        hunter.create_cloud_cycle = lambda iteration: ("hunting-cloud", "folder-1")

        cloud_id, folder_id = hunter.ensure_hybrid_address_scope(1)

        self.assertEqual((cloud_id, folder_id), ("hunting-cloud", "folder-1"))

    def test_hybrid_never_deletes_service_cloud(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {"service_cloud_id": "service-cloud"}

        self.assertFalse(hunter.can_delete_hybrid_cloud("service-cloud"))
        self.assertTrue(hunter.can_delete_hybrid_cloud("hunting-cloud"))

    def test_cloud_delete_cleanup_settings_exist(self):
        text = MODULE_PATH.read_text(encoding="utf-8")
        self.assertIn("address_delete_retries", text)
        self.assertIn("pre_cloud_delete_cleanup_sleep_seconds", text)

    def test_telegram_notification_setting_exists(self):
        text = MODULE_PATH.read_text(encoding="utf-8")
        self.assertIn("notify_success", text)
        self.assertIn("TELEGRAM_BOT_TOKEN", text)
        self.assertIn("--test-telegram", text)


class StateTrackingTests(unittest.TestCase):
    def test_allocate_and_classify_stops_on_each_target_prefix(self):
        class FakeClient:
            def __init__(self, ip):
                self.ip = ip

            def reserve_external_ipv4(self, **kwargs):
                return {
                    "id": f"addr-{self.ip.replace('.', '-')}",
                    "externalIpv4Address": {
                        "address": self.ip,
                        "zoneId": "ru-central1-a",
                    },
                }

        for ip in [
            "51.250.77.87",
            "84.201.188.1",
            "95.161.10.10",
            "178.154.20.20",
            "130.193.30.30",
        ]:
            with self.subTest(ip=ip):
                hunter = object.__new__(yc.IpHunter)
                hunter.config = {
                    "zone": "ru-central1-a",
                    "zones": ["ru-central1-a"],
                    "target_ips": [],
                    "target_cidrs": [
                        "51.250.0.0/16",
                        "84.201.0.0/16",
                        "95.161.0.0/16",
                        "178.154.0.0/16",
                        "130.193.0.0/16",
                    ],
                    "create_address_permission_retries": 1,
                }
                hunter.state = {}
                hunter.client = FakeClient(ip)
                hunter.dry_run = False
                hunter.persist_state = lambda: None

                result = hunter.allocate_and_classify("cloud-1", "folder-1", 1, 1)

                self.assertIsNotNone(result)
                self.assertEqual(result.ip, ip)

    def test_track_cloud_address_deduplicates(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.state = {}
        hunter.track_cloud_address("cloud-1", "addr-1", "198.51.100.10")
        hunter.track_cloud_address("cloud-1", "addr-1", "198.51.100.10")
        got = hunter.state["addresses_by_cloud"]["cloud-1"]
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["address_id"], "addr-1")

    def test_cleanup_retries_even_when_delete_was_submitted(self):
        class FakeClient:
            def __init__(self):
                self.deleted = []

            def delete_address(self, address_id, wait=True):
                self.deleted.append((address_id, wait))

        hunter = object.__new__(yc.IpHunter)
        hunter.state = {
            "addresses_by_cloud": {
                "cloud-1": [
                    {"address_id": "addr-1", "delete_submitted": True},
                    {"address_id": "addr-1", "delete_submitted": True},
                ]
            }
        }
        hunter.config = {
            "address_delete_retries": 3,
            "address_delete_retry_sleep_seconds": 0,
            "pre_cloud_delete_cleanup_sleep_seconds": 0,
        }
        hunter.client = FakeClient()
        hunter.dry_run = False
        hunter.persist_state = lambda: None
        hunter.sleep_backoff = lambda seconds: None

        hunter.cleanup_cloud_addresses("cloud-1")

        self.assertEqual(hunter.client.deleted, [("addr-1", False)])

    def test_address_rate_limit_raises_rate_limit_hit(self):
        class FakeClient:
            def reserve_external_ipv4(self, **kwargs):
                raise yc.ApiError(429, 8, "Quota limit vpc.externalAddressesCreation.rate exceeded")

        hunter = object.__new__(yc.IpHunter)
        hunter.config = {"zone": "ru-central1-a", "zones": ["ru-central1-a"]}
        hunter.client = FakeClient()

        with self.assertRaises(yc.RateLimitHit):
            hunter.allocate_and_classify("cloud-1", "folder-1", 1, 1)

    def test_save_success_calls_notification(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {"open_success_video": False}
        hunter.state = {}
        hunter.persist_state = lambda: None
        called = []
        hunter.notify_success = lambda result: called.append(result.ip)

        hunter.save_success(
            yc.AttemptResult(
                ip="198.51.100.10",
                zone="ru-central1-a",
                address_id="addr-1",
                cloud_id="cloud-1",
                folder_id="folder-1",
            )
        )

        self.assertEqual(called, ["198.51.100.10"])
        self.assertEqual(hunter.state["success"]["ip"], "198.51.100.10")

    def test_open_success_video_can_be_disabled(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {"open_success_video": False}

        self.assertFalse(hunter.open_success_video())

    def test_open_success_video_uses_default_url(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {}
        opened = []
        original_open = yc.webbrowser.open
        yc.webbrowser.open = lambda url, new=0, autoraise=True: opened.append(
            (url, new, autoraise)
        ) or True
        try:
            self.assertTrue(hunter.open_success_video())
        finally:
            yc.webbrowser.open = original_open

        self.assertEqual(len(opened), 1)
        self.assertEqual(opened[0][1:], (2, True))
        self.assertTrue(opened[0][0].startswith("file:///"))

    def test_success_video_launcher_sets_youtube_volume(self):
        launch_url = yc.build_success_video_launcher(yc.SUCCESS_VIDEO_URL)
        self.assertTrue(launch_url.startswith("file:///"))
        parsed = urllib.parse.urlparse(launch_url)
        html = Path(urllib.request.url2pathname(parsed.path)).read_text(encoding="utf-8")
        self.assertIn("tiCIjTNARX8", html)
        self.assertIn("PLCZl9PrJVBkSJGJi3zpDkbxy8X-BeUQvK", html)
        self.assertIn("setVolume(100)", html)

    def test_telegram_enabled_inside_disabled_parent_still_sends(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {
            "notifications": {
                "enabled": False,
                "telegram": {
                    "enabled": True,
                    "bot_token": "token",
                    "chat_id": "chat",
                },
            }
        }
        calls = []
        original_http_json = yc.http_json
        yc.http_json = lambda method, url, body, token, timeout=60: calls.append(
            (method, url, body, token, timeout)
        ) or {"ok": True}
        try:
            hunter.notify_success(
                yc.AttemptResult(
                    ip="198.51.100.10",
                    zone="ru-central1-a",
                    address_id="addr-1",
                    cloud_id="cloud-1",
                    folder_id="folder-1",
                )
            )
        finally:
            yc.http_json = original_http_json

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "POST")
        self.assertIn("198.51.100.10", calls[0][2]["text"])

    def test_telegram_test_message_uses_fake_attempt_result(self):
        hunter = object.__new__(yc.IpHunter)
        hunter.config = {
            "zone": "ru-central1-a",
            "notifications": {
                "telegram": {
                    "enabled": True,
                    "bot_token": "token",
                    "chat_id": "chat",
                },
            },
        }
        calls = []
        original_http_json = yc.http_json
        yc.http_json = lambda method, url, body, token, timeout=60: calls.append(body) or {"ok": True}
        try:
            self.assertTrue(hunter.test_telegram_notification())
        finally:
            yc.http_json = original_http_json

        self.assertEqual(len(calls), 1)
        self.assertIn("telegram-test", calls[0]["text"])


if __name__ == "__main__":
    unittest.main()
