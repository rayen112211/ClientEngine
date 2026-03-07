import smtplib
import unittest

from email_engine import _classify_smtp_data_error


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


if __name__ == '__main__':
    unittest.main()
