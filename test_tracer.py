import unittest
from cursor_tracker import CursorTracker
import tracer2

class TestTracer(unittest.TestCase):
    def test_process_file(self):
        tracker = CursorTracker(None)
        tracer2.process_file(tracker, 'tests/simple_trace.trc')

if __name__ == '__main__':
    unittest.main()
