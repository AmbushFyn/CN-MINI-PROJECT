import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from worker import execute

class TaskTests(unittest.TestCase):
    def test_add(self):
        self.assertEqual(execute({"task":"add","payload":{"numbers":[1,2,3]}}), 6)
    def test_multiply(self):
        self.assertEqual(execute({"task":"multiply","payload":{"numbers":[2,3,4]}}), 24)
    def test_uppercase(self):
        self.assertEqual(execute({"task":"uppercase","payload":{"text":"tcp"}}), "TCP")
    def test_word_count(self):
        self.assertEqual(execute({"task":"word_count","payload":{"text":"one two three"}}), 3)

if __name__ == "__main__":
    unittest.main()
