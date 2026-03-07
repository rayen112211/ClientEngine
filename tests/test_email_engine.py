import smtplib
import unittest
from unittest.mock import patch

from email_engine import _classify_smtp_data_error, send_email


class EmailEngineRateLimitTests(unittest.TestCase):
    def test_temporary_4xx_is_rate_limited(self):
        exc = smtplib.SMTPDataError(451, b'4.7.1 Please try again later')
        is_rate, is_bounce = _classify_smtp_data_error(exc)
        self.assertTrue(is_rate)
        self.assertFalse(is_bounce)

    def test_user_unknown_is_hard_bounce(self):
        exc = smtplib.SMTPDataError(550, b'5.1.1 user unknown')
        is_rate, is_bounce = _classify_smtp_data_error(exc)
        self.assertFalse(is_rate)
        self.assertTrue(is_bounce)

    def test_policy_reject_not_auto_rate_limited(self):
        exc = smtplib.SMTPDataError(554, b'5.7.1 Message rejected due to policy')
        is_rate, is_bounce = _classify_smtp_data_error(exc)
        self.assertFalse(is_rate)
        self.assertFalse(is_bounce)

    def test_421_without_quota_hint_not_auto_rate_limited(self):
        exc = smtplib.SMTPDataError(421, b'4.3.2 Service not available, closing transmission channel')
        is_rate, is_bounce = _classify_smtp_data_error(exc)
        self.assertFalse(is_rate)
        self.assertFalse(is_bounce)


class SendEmailReliabilityTests(unittest.TestCase):
    def _settings(self):
        return {
            "smtp_host": "smtp.example.com",
            "smtp_port": "465",
            "smtp_user": "user@example.com",
            "smtp_password": "pw",
            "smtp_use_ssl": "true",
            "from_name": "Tester",
            "from_email": "user@example.com",
            "reply_to": "user@example.com",
            "imap_sync_sent": "true",
        }

    def test_timeout_error_marked_transient(self):
        with patch("email_engine.smtplib.SMTP_SSL", side_effect=TimeoutError("timed out")):
            success, error, is_bounce, is_rate = send_email(
                "lead@example.com",
                "Subject",
                "Body",
                self._settings(),
            )
        self.assertFalse(success)
        self.assertFalse(is_bounce)
        self.assertFalse(is_rate)
        self.assertIn("Transient SMTP error", error or "")

    def test_imap_sent_folder_timeout_does_not_fail_send(self):
        class _FakeSMTP:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def login(self, *_args, **_kwargs):
                return None

            def send_message(self, *_args, **_kwargs):
                return None

        with patch("email_engine.smtplib.SMTP_SSL", return_value=_FakeSMTP()):
            with patch("email_engine.imaplib.IMAP4_SSL", side_effect=TimeoutError("imap timeout")):
                with patch("builtins.print"):
                    success, error, is_bounce, is_rate = send_email(
                        "lead@example.com",
                        "Subject",
                        "Body",
                        self._settings(),
                    )
        self.assertTrue(success)
        self.assertIsNone(error)
        self.assertFalse(is_bounce)
        self.assertFalse(is_rate)


if __name__ == '__main__':
    unittest.main()
