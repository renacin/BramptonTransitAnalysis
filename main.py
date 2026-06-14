# Name:                                            Renacin Matadeen
# Date:                                               06/14/2026
# Title                              Main Logic Of Scheduler & Main Entry Point Of Code
#
# ----------------------------------------------------------------------------------------------------------------------
import time
import threading
from datetime import datetime, timedelta

from Functions.env_config      import *
from Functions.env_setup       import *
from Functions.gtfs_downloader import *
from Functions.data_helper     import *
from Functions.data_exporter   import *
from Functions.data_collect    import *

stop_event = threading.Event()
# ----------------------------------------------------------------------------------------------------------------------


# Find Out How Many Seconds Until The Export Window
def seconds_until(hour_, minute_):
    """ How Many Seconds Until Time Window? """

    # Find Current Time & Window
    now = datetime.now()
    target = now.replace(hour=hour_, minute=minute_, second=0, microsecond=0)

    # If Window Already Passed Find Tomorrows
    if now > target:
        target = target + timedelta(days=1)

    # Find Total Seconds
    diff = target - now
    return int(diff.total_seconds()) + 1



# Create Scheduled Behaviour For: Data Collection 
def data_collector_scheduler():
    """ Instantiate Data Collector & Start Main Loop """

    # Start Data Collector
    DataCollector = Collector()
    while not stop_event.is_set():
        try:
            DataCollector.get_bus_loc()
        except Exception as e:
            pass
        finally:
            stop_event.wait(15)



# Create Scheduled Behaviour For: Data Exporter
def data_exporter_scheduler():
    """ Instantiate Data Exporter & Start Main Loop """

    # Start The Data Exporter
    DataExporter = Exporter()

    # Main Loop Checking If It's 2:30AM, Sleep Until Then, Then Export, The Wait 30 Min, Repeat
    while not stop_event.is_set():
        stop_event.wait(seconds_until(hour_=2, minute_=30))
        DataExporter.export_all()
        stop_event.wait(1800)



# Create Scheduled Behaviour For: GTFS Downloader
def gtfs_dowloader_scheduler():
    """ Instantiate GTFS Downloader & Start Main Loop """

    # Start The Data Exporter
    GTFS_Getter = GTFS_Downloader()

    # Main Loop Checking If It's 12:30PM, Sleep Until Then, Then Export, The Wait 30 Min, Repeat
    while not stop_event.is_set():
        stop_event.wait(seconds_until(hour_=12, minute_=30))
        GTFS_Getter.gather_GTFS()
        stop_event.wait(1800)



# ----------------------------------------------------------------------------------------------------------------------
# The Main Function Will Run Each Sub Function As It's Own Process & Have Error Catching For Graceful Shut Down
def main():

    # Step #1 Prepare Folders With Environment Setup
    EnvSetup = EnvConfig()
    EnvSetup.setup()

    # Define Each Process, They Should Be Their Own Thread And Run Independently
    threads = [threading.Thread(target = data_collector_scheduler, name="DataCollector",  daemon=True),
               threading.Thread(target = data_exporter_scheduler,  name="DataExporter",   daemon=True),
               threading.Thread(target = gtfs_dowloader_scheduler, name="GTFSDownloader", daemon=True),]
 
    # Start Each Thread
    for t in threads:
        t.start()
 
    # Main Loop Of Thread (Keep Looking For A Kill Signal)
    try:
        while True:
            time.sleep(1)
 
    except KeyboardInterrupt:
        stop_event.set()
        for t in threads:
            t.join(timeout=30)
 


# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    main()
