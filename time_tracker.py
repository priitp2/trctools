from datetime import datetime, timedelta
from typing import Optional

class TimeTracker:
    '''TimeTracker keeps track of the wall clock and time deltas between database
       calls. It generates wall clock readings.'''
    wall_clock: Optional[datetime] = None
    first_tim: Optional[int] = None
    current_tim: Optional[int] = None
    tim_delta: timedelta = timedelta()

    def reset(self, wallclock: datetime) -> None:
        '''Sets new wall clock reading and resets rest of the variables '''
        self.wall_clock: datetime = wallclock
        self.first_tim = None
        self.current_tim = None
        self.tim_delta: timedelta = timedelta()
    def get_wc(self, tim: int) -> datetime:
        ''' Gets new wall clock reading for the tim'''
        if not self.wall_clock:
            print(f"wall_clock hasn't been set, tim = {tim}")
            return None

        if tim is None:
            if self.first_tim is not None:
                return self.wall_clock + self.tim_delta
            return self.wall_clock

        if not self.first_tim:
            self.first_tim = tim
            self.current_tim = tim
            return self.wall_clock

        self.current_tim = tim
        self.tim_delta = timedelta(microseconds = self.current_tim - self.first_tim)
        return self.wall_clock + self.tim_delta
