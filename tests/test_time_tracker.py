import unittest
from datetime import datetime
from datetime import timedelta
from time_tracker import TimeTracker

TIM0 = 7795634107774

class TestTimeTracker(unittest.TestCase):
    '''Tests for the class TimeTracker'''
    def test_init(self):
        tracker = TimeTracker()
        self.assertIs(tracker.wall_clock, None)
        self.assertIs(tracker.first_tim, None)
        self.assertIs(tracker.current_tim, None)
        self.assertEqual(tracker.tim_delta, timedelta())
    def test_reset(self):
        tracker = TimeTracker()
        clock = datetime.today()
        tracker.reset(clock)
        self.assertEqual(tracker.wall_clock, clock)

        # TimeTracker.reset() sets *tim* properties to None
        tracker.get_wc(TIM0)
        tracker.reset(clock)
        self.assertIs(tracker.first_tim, None)
        self.assertIs(tracker.current_tim, None)
        self.assertEqual(tracker.tim_delta, timedelta())
    def test_get_wc(self):
        tracker = TimeTracker()

        # wall clock hasn't been set yet, so there's nothing to return
        with self.assertRaises(ValueError):
            tracker.get_wc(None)

        # 2023-05-18T13:56:18.679265+02:00
        clock = datetime(2023, 5, 18, 13, 56, 18, 679265)
        tracker.reset(clock)

        clock0 = tracker.get_wc(TIM0)
        self.assertEqual(clock, clock0)
        self.assertEqual(tracker.first_tim, TIM0)
        self.assertEqual(tracker.current_tim, TIM0)
        self.assertEqual(tracker.tim_delta, timedelta())

        tim1 = 7795634107865
        delta = timedelta(microseconds = tim1 - TIM0)
        clock1 = tracker.get_wc(tim1)
        self.assertEqual(clock1, clock0 + delta)
        self.assertEqual(tracker.first_tim, TIM0)
        self.assertEqual(tracker.current_tim, tim1)
        self.assertEqual(tracker.tim_delta, delta)
if __name__ == '__main__':
    unittest.main()
