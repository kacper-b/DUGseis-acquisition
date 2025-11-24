from datetime import datetime, timedelta, timezone
import sys, re, logging

class GPSyncError(Exception):
    pass

class GPSTimingFactorCalculator:
    def __init__(self, file_name, logger=logging.getLogger(), acceptable_delay_sec=600):
        self.file_name = file_name
        self.logger = logger
        self.acceptable_delay_sec = acceptable_delay_sec
        self.last_gps_sync_time = None
        self.set_GPS_sync_time()

    def set_GPS_sync_time(self):
        try:
            with open(self.file_name) as f:
                t = f.read()
                self.logger.debug(f"Read GPS sync file content: {t}")
        except FileNotFoundError as e:
            raise GPSyncError(f"GPS sync file {self.file_name} not found.") from e
        regex = r".*(?:Last sync:\s+\w+,\s+|^)(20\d\d-\d\d-\d\d\s+\d\d\:\d\d\:[\.\d]+).*\s*\w*\s+\(UTC([\+\-]\d)h\)"
        match = re.match(regex, t, re.DOTALL)
        if not match: 
            raise GPSyncError(f"Could not parse GPS sync time from file {self.file_name}.")
        time_of_last_gps_sync_local = datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S.%f') 
        utc_offset = timedelta(hours=float(match.group(2)))
        utc_time_of_last_gps_sync = (time_of_last_gps_sync_local - utc_offset)
        self.logger.debug(f"Parsed GPS sync time: {utc_time_of_last_gps_sync} UTC")
        self.last_gps_sync_time = utc_time_of_last_gps_sync
    
    def is_sync_recent(self, current_utc_time, refresh_file=False) -> bool:
        if refresh_file:
            self.set_GPS_sync_time()
        time_diff = (current_utc_time - self.last_gps_sync_time).total_seconds()
        self.logger.debug(f"Current time: {current_utc_time} UTC")
        self.logger.debug(f"GPS sync time: {self.last_gps_sync_time} UTC")
        self.logger.debug(f"Time since last GPS sync: {time_diff} seconds")
        if time_diff <= 0:
            raise GPSyncError("GPS sync time is in the future compared to current time.")
        return time_diff <= self.acceptable_delay_sec
    
    def get_timing_quality(self, current_utc_time, refresh_file=False) -> bool:
        if refresh_file:
            self.set_GPS_sync_time()
        time_diff = (current_utc_time - self.last_gps_sync_time).total_seconds()
        if time_diff <= 0:
            raise GPSyncError("GPS sync time is in the future compared to current time.")
        return max(0, int(100 - round(time_diff/3600)))
    


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    gps_checker = GPSTimingFactorCalculator(sys.argv[1])
    print(gps_checker.is_sync_recent())
