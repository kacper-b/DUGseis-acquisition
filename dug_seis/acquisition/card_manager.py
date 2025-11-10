# DUG-Seis
#
# :copyright:
#    ETH Zurich, Switzerland
# :license:
#    GNU Lesser General Public License, Version 3
#    (https://www.gnu.org/copyleft/lesser.html)
#
"""Manages the different hardware components. Calls the data transfer periodically.
- state machine
- restart
- help to hardware problems
- simulation of data
"""
import time
import logging
import copy

from obspy.core import UTCDateTime
from dug_seis.acquisition.one_card import Card
from dug_seis.acquisition.star_hub import StarHub
from dug_seis.acquisition.data_to_asdf import DataToASDF
from dug_seis.acquisition.hardware_mockup import SimulatedHardware
from dug_seis.acquisition.gps_synch_check import GPSTimingFactorCalculator, GPSyncError
import dug_seis.acquisition.streaming as streaming

logger = logging.getLogger('dug-seis')

def update_timing_based_on_gps_sync(gps_checker, data_to_asdf, previous_gps_sync_status):
    is_gps_sync_old = not gps_checker.is_sync_recent(data_to_asdf.time_stamps.starttime_UTCDateTime().datetime.replace(tzinfo=None))

    if is_gps_sync_old and not previous_gps_sync_status:
        logger.debug("GPS sync is too old!")
    elif is_gps_sync_old and previous_gps_sync_status:
        logger.warning("GPS sync is lost!")
    elif not is_gps_sync_old and previous_gps_sync_status:
        logger.debug("GPS sync is OK!")
    elif not is_gps_sync_old and not previous_gps_sync_status:
        logger.info("GPS sync is back to normal, restarting timing...")
        old = data_to_asdf.time_stamps.starttime_UTCDateTime()
        data_to_asdf.set_starttime_now()
        new = data_to_asdf.time_stamps.starttime_UTCDateTime()
        logger.info(f"GPS sync is back to normal, timming corrected by: {new-old} sec")
    return not is_gps_sync_old

def run(param):
    """
    Main acquisition loop, run's until ctrl + c.
    """
    bytes_per_transfer = param['Acquisition']['bytes_per_transfer']
    bytes_per_stream_packet = param['Acquisition']['bytes_per_stream_packet']
    simulation_mode = param['Acquisition']['simulation_mode']
    check_GPS_sync = param['Acquisition']['check_GPS_sync']
    # make classes
    card1 = Card(param, 0)
    card2 = Card(param, 1)
    star_hub = StarHub()

    # simulate hardware if in simulation mode
    if simulation_mode:
        simulated_hardware1 = SimulatedHardware(param)
        simulated_hardware2 = SimulatedHardware(param)
        simulated_hardware1.mock_card(card1)
        simulated_hardware2.mock_card(card2)
        simulated_hardware1.mock_starhub(star_hub)

    # try close, in case the last run was aborted ...
    card1.close()
    card2.close()
    star_hub.close()

    # init setup
    card1.init_card(param)
    card2.init_card(param)
    star_hub.init_star_hub([card1, card2])

    # read xio, for testing purpose, enable inputs in one_card_std_init.py
    # while True:
    #    logger.info("xio l_data, card1: {0:b}, card2: {1:b}".format(card1.read_xio(), card2.read_xio()))
    #    time.sleep(0.1)
    if check_GPS_sync:
        gps_checker = GPSTimingFactorCalculator(param['Acquisition']['gps_sync_file'], logger=logger, acceptable_delay_sec=600)
        logger.info("GPS sync checking enabled.")
    else:
        gps_checker = None
        logger.info("GPS sync checking disabled.")

    star_hub.start()
    data_to_asdf = DataToASDF(param)
    if data_to_asdf.error:
        logger.error("an error occurred, closing cards.")
        card1.close()
        card2.close()
        star_hub.close()
        exit(1)

    #
    # start the data streaming servers
    #
    servers = streaming.create_servers(param)
    for server in servers:
        server.start()

    # wait?
    # card1.wait_for_data()
    # card2.wait_for_data()

    # read status, no actions planned at the moment
    # the read status function will print() if there is a problem ...
    card1.read_status()
    card2.read_status()


    logger.info("Setup complete, waiting for Trigger...")
    while not card1.trigger_received():
        pass


    data_to_asdf.set_starttime_now()

    logger.info("Acquisition started...")
    def stream_data():
        time_stamp_this_loop = time.perf_counter()

        stream_ts = copy.copy(data_to_asdf.time_stamps)
        bytes_streamed = 0
        t_stream = 0
                #
                # polling scheme here, might not be the best?
                #
        card1_bytes_available = card1.nr_of_bytes_available()
        card2_bytes_available = card2.nr_of_bytes_available()
                # logger.info("card1_bytes_available: {}, {} Mb".format(card1_bytes_available, card1_bytes_available / 1024 / 1024))
                # logger.info("card2_bytes_available: {}, {} Mb".format(card2_bytes_available, card2_bytes_available / 1024 / 1024))

                #
                # handle streaming: send data packets until all the available bytes
                # have been consumed or less than bytes_per_stream_packet are left
                #
        while (card1_bytes_available >= bytes_streamed + bytes_per_stream_packet and
                    card2_bytes_available >= bytes_streamed + bytes_per_stream_packet):
            _tref = time.perf_counter()

            cards_data = [card1.read_data(bytes_per_stream_packet, bytes_streamed),
                                card2.read_data(bytes_per_stream_packet, bytes_streamed)]
            timing_quality = 0
            if check_GPS_sync:
                try:
                    timing_quality = gps_checker.get_timing_quality(stream_ts.starttime_UTCDateTime().datetime.replace(tzinfo=None), refresh_file=False)
                except GPSyncError as e:
                    logger.error(f"Error while checking GPS sync: {e}")
                logger.debug(f"Timing quality: {timing_quality}")
            streaming.feed_servers(param, servers, cards_data, stream_ts.starttime_UTCDateTime(), timing_quality)
            stream_ts.set_starttime_next_segment( int(cards_data[0].size / 16) )
            bytes_streamed += bytes_per_stream_packet

            t_stream += time.perf_counter()-_tref # elapsed time

                #
                # handle file generation: create files when enough data is available
                #
        if (card1_bytes_available >= bytes_per_transfer and
                    card2_bytes_available >= bytes_per_transfer):
                    #
                    # Log system vs data time
                    #
            logger.info("Data time {} sys/data time difference: {} sec".format(
                        stream_ts.starttime_UTCDateTime(), UTCDateTime()-stream_ts.starttime_UTCDateTime()))

            _tref = time.perf_counter()
            card1.read_status()     # writes overrun error to logger.error
                    # don't read 2nd card status, it resets the overflow error, the card continues to generate data then
                    # card2.read_status()
                    # logger.info("read_status(): {}".format(card1.read_status()))
            data_to_asdf.data_to_asdf([card1.read_data(bytes_per_transfer, 0),
                                            card2.read_data(bytes_per_transfer, 0)])
            card1.data_has_been_read()
            card2.data_has_been_read()

                    #
                    # streaming time sync due to sample dropping logic in data_to_asdf
                    # this will cause the next sample to have the same timestamp as the
                    # last sent sample. The software downstram will decide how to handle
                    # this
                    #
            bytes_streamed -= bytes_per_transfer
            if bytes_streamed == 0: # streamed data and data writted to asdf files is the same amount
                if stream_ts.starttime_UTCDateTime() != data_to_asdf.time_stamps.starttime_UTCDateTime():
                    stream_ts = copy.copy(data_to_asdf.time_stamps) # align timestamps with asdf
                    logger.info("Aligned streaming timestamps with asdf files")

            now = time.perf_counter()
            t_asdf = now - _tref
            t_loop = now - time_stamp_this_loop
            logger.info("Loop took: {:.2f} sec (asdf {:.2f} + stream {:.2f} -> {}%)"
                                .format(t_loop, t_asdf, t_stream, int((t_asdf + t_stream)/t_loop * 100)))
            t_stream = 0
            time_stamp_this_loop = now
        else:
            time.sleep(0.1)

    try:
        previous_gps_sync_status = False
        while True:
            if check_GPS_sync:
                try:
                    gps_checker.set_GPS_sync_time()
                    previous_gps_sync_status = update_timing_based_on_gps_sync(gps_checker, data_to_asdf, previous_gps_sync_status)
                except Exception as e:
                    logger.error(f"Error while checking GPS sync: {e}")
            stream_data()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected, exiting...")
    finally:
        for server in servers:
            server.stop()

