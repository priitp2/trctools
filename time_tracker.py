from datetime import timedelta
class TimeTracker:
    '''TimeTracker keeps track of the wall clock and time deltas between database
       calls. It generates wall clock readings.'''
    wall_clock = None
    first_tim = None
    current_tim = None
    tim_delta = timedelta()

    def reset(self, wallclock):
        '''Sets new wall clock reading and resets rest of the variables '''
        self.wall_clock = wallclock
        self.first_tim = None
        self.current_tim = None
        self.tim_delta = timedelta()
    def get_wc(self, tim):
        ''' Gets new wall clock reading for the tim'''
        if not self.wall_clock:
            raise ValueError("wall_clock hasn't been set")
        if not self.first_tim:
            self.first_tim = tim
            self.current_tim = tim
            return self.wall_clock
        self.current_tim = tim
        self.tim_delta = timedelta(microseconds = self.current_tim - self.first_tim)
        return self.wall_clock + self.tim_delta
