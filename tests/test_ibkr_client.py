import unittest
from unittest.mock import Mock, patch

from ibkr_client import IBKRAPIError, IBKRClient, IBKRConfig, mask_account_id


class IBKRClientTests(unittest.TestCase):
    def test_portfolio_uses_first_visible_account_and_paginates(self):
        responses = [
            [{"accountId": "U1234567", "currency": "CHF"}],
            [{"ticker": "AAA"}],
            [],
            {"netliquidation": {"amount": 1000, "currency": "CHF"}},
            {"BASE": {"cashbalance": 100, "currency": "BASE"}},
        ]
        fake = Mock()
        fake.ok = True
        fake.status_code = 200
        fake.json.side_effect = responses

        with patch("requests.Session.get", return_value=fake):
            payload = IBKRClient(IBKRConfig(base_url="https://example.test/v1/api")).portfolio()

        self.assertEqual(payload["account_id"], "U1234567")
        self.assertEqual(payload["positions"][0]["ticker"], "AAA")

    def test_auth_error_is_safe(self):
        fake = Mock(ok=False, status_code=401)
        with patch("requests.Session.get", return_value=fake):
            with self.assertRaisesRegex(IBKRAPIError, "authentication expired"):
                IBKRClient(IBKRConfig(base_url="https://example.test/v1/api")).accounts()

    def test_market_snapshot_retries_after_preflight(self):
        preflight = Mock(ok=True, status_code=200)
        preflight.json.return_value = [{"conid": 123}]
        quote = Mock(ok=True, status_code=200)
        quote.json.return_value = [{"conid": 123, "31": "101.25"}]
        with patch("requests.Session.get", side_effect=[preflight, quote]) as get:
            rows = IBKRClient(IBKRConfig(base_url="https://example.test/v1/api")).market_snapshot([123])
        self.assertEqual(rows[0]["31"], "101.25")
        self.assertEqual(get.call_count, 2)

    def test_mask_account_id(self):
        self.assertEqual(mask_account_id("U1234567"), "••••4567")


if __name__ == "__main__":
    unittest.main()
