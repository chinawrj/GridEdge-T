import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("audit", Path(__file__).with_name("audit-szse-amounts.py"))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class ExactAmounts(unittest.TestCase):
    def test_preserves_cents_beyond_float_precision(self):
        value = audit.decode(b"9007199254740991.01")
        self.assertEqual(audit.cents(value), 900719925474099101)
        # Regression: ordinary JSON float parsing irreversibly loses the cent.
        import json
        self.assertNotEqual(str(json.loads(b"9007199254740991.01")), str(value))

    def test_rejects_nonfinite_duplicate_and_fractional_cents(self):
        for raw in (b"NaN", b'{"amount":1,"amount":2}'):
            with self.assertRaises(ValueError):
                audit.decode(raw)
        for raw in (b"1.001", b"-1", b"true"):
            with self.assertRaises(ValueError):
                audit.cents(audit.decode(raw))

    def test_exact_mismatch_is_visible(self):
        raw = ('{"code":"0","data":{"code":"002256","name":"兆新股份",'
               '"amount":9007199254740991.01,"volume":1,"picupdata":'
               '[["15:00","3","3","0","0",1,9007199254740991.02]]}}').encode()
        result = audit.audit(raw)
        self.assertFalse(result["totals_equal"])
        self.assertFalse(result["admitted"])


if __name__ == "__main__":
    unittest.main()
