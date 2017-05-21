from datetime import datetime, timedelta

development_mode = True

class Settings:
    def __init__(self):
        self._now = datetime.now()
        self._now_utc = datetime.utcnow()
        self._do_hourly = self._now.timetuple().tm_min is 0 or development_mode
        self._do_daily = True if (self._now_utc.timetuple().tm_hour == 11 and self._do_hourly) or development_mode else False
        self._workers = 2 # Number of threads to use in multi processing

        if self._do_hourly:
            print("Settings: Performing hourly tasks")
        if self._do_daily:
            print("Settings: Performing daily tasks")

    @property
    def now(self):
        return self._now

    @property
    def utcnow(self):
        return self._now_utc

    @property
    def is_hourly(self):
        return self._do_hourly

    @property
    def is_daily(self):
        return self._do_daily

    @property
    def workers(self):
        return self._workers

    @property
    def hour_offset_utc(self):
        return self._now_utc - timedelta(hours=1)

    @property
    def minute_data_prune_time(self):
        return self._now_utc - timedelta(seconds=64800) # 24 hours

    @property
    def hourly_data_prune_time(self):
        return self._now_utc - timedelta(seconds=604800) # 1 week

    @property
    def standard_headers(self):
        return {'user_agent': 'https://eve.exchange'}
