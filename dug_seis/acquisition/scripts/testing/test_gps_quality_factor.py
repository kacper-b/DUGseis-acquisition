from dug_seis.acquisition.gps_synch_check import GPSTimingFactorCalculator
from datetime import datetime
import os
import io
import pytest
from unittest.mock import mock_open, patch

class Test_GPSTimingFactorCalculator:

    

    def test_should_return_UTC_corrected_time_postive_offset(self):
        # t = patch("dug_seis.acquisition.gps_synch_check.open")
        m = mock_open(read_data="Last sync: Mon, 2023-03-20 12:34:56.789 (UTC+1h)\n")
        with patch("builtins.open", m):
            qc = GPSTimingFactorCalculator("TMP_test_gps_sync.txt")
            qc.set_GPS_sync_time()
        
            result = qc.last_gps_sync_time
            expected = datetime(2023, 3, 20, 11, 34, 56, 789000)
            assert result == expected

    
    def test_should_return_UTC_corrected_time_negative_offset(self):
        m = mock_open(read_data="Last sync: Mon, 2023-03-20 12:34:56.779 (UTC-1h)\n")
        with patch("builtins.open", m):
            qc = GPSTimingFactorCalculator('TMP_test_gps_sync.txt')
            qc.set_GPS_sync_time()
        
            result = qc.last_gps_sync_time
            expected = datetime(2023, 3, 20, 13, 34, 56, 779000)
            assert result == expected

    def test_should_correct_time_from_awk_parsed_input(self):
        m = mock_open(read_data="2025-11-04 12:56:00.00 (UTC+1h)")
        with patch("builtins.open", m):
            qc = GPSTimingFactorCalculator('dug_seis/acquisition/scripts/testing/data/GPS_awk_parsed_status.txt')
            qc.set_GPS_sync_time()
            expected = datetime(2025, 11, 4, 11, 56, 0, 0)
            assert qc.last_gps_sync_time == expected
    
    def test_should_correct_time_from_raw_input(self):
        content  = """
        mbgstatus v4.2.18 copyright Meinberg 2001-2021

        GPS180PEX 029512011700 (FW 2.48, ASIC 8.06) at port 0x4000, irq 16
        Warm Boot, 0 GPS sats tracked, 7 expected to be visible
        Date/time:  Tu, 2025-11-04  14:32:11.31 CET (UTC+1h)
        Status info: Input signal available
        Status info: *** Time not synchronized
        Status info: *** Receiver pos. not verified
        Last sync:  Tu, 2025-11-06  17:59:00.00 CET (UTC+1h)
        Receiver Position:  lat: +46.4969, lon: +8.4954, alt: 1530m
        Current GPS/UTC offset: 18 s, no leap second announced."""
        m = mock_open(read_data=content)
        with patch("builtins.open", m):
            qc = GPSTimingFactorCalculator('dug_seis/acquisition/scripts/testing/data/GPS_full_output.txt')
            qc.set_GPS_sync_time()
            expected = datetime(2025, 11, 6, 16, 59, 0, 0)
            assert qc.last_gps_sync_time == expected
            
    def test_should_assign_timing_quality_factor_as_100_minus_hours_elapsed_since_sync(self):

        m = mock_open(read_data="Last sync: Mon, 2025-11-04 13:01:00.000 (UTC+1h)\n")
        with patch("builtins.open", m):
            qc = GPSTimingFactorCalculator('TMP_test_gps_sync.txt')
            qc.set_GPS_sync_time()
            
            assertion_cases = { # given in UTC time
                datetime(2025, 11, 4, 12, 1+30, 0, 0): ["30min  after sync", 100], 
                datetime(2025, 11, 4, 12, 1+31, 0, 0): ["31min  after sync", 99], 
                datetime(2025, 11, 4, 13, 1, 0, 0): ["1 hour  after sync", 99], 
                datetime(2025, 11, 4, 16, 1, 0, 0): ["4 hours  after sync", 96], 
                datetime(2025, 11, 5, 12, 1, 0, 0): ["24 hours  after sync", 76], 
                datetime(2025, 11, 8, 12, 1, 0, 0): ["4 days  after sync", 4], 
                datetime(2025, 11, 8, 16, 1, 0, 0): ["100h  after sync", 0], 
                datetime(2025, 11, 8, 17, 1, 0, 0): ["more than 100 hours (i.e. 101h) after sync", 0]
            }

            for test_time, case_desc_rslt in assertion_cases.items():
                case_desc, expected_quality = case_desc_rslt
                timing_quality = qc.get_timing_quality(test_time)
                assert timing_quality == expected_quality, f"Failed for case: {case_desc}, expected {expected_quality}, got {timing_quality}"  
