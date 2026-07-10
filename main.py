# Name:                                            Renacin Matadeen
# Date:                                               06/14/2026
# Title                              Main Logic Of Scheduler & Main Entry Point Of Code
#
# ----------------------------------------------------------------------------------------------------------------------
import time
import threading
from datetime import datetime, timedelta

from Functions.env_config         import *
from Functions.env_setup          import *
from Functions.gtfs_downloader    import *
from Functions.data_helper        import *
from Functions.data_exporter      import *
from Functions.data_collect       import *
from Functions.data_visualiser    import *
from Functions.upld_dropbox       import *

# Keyboard Shortcut Can Trigger This - Be Careful!
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

    # Main Loop Checking If It's 2:30AM, Sleep Until Then, Then Export, The Wait 30 Min, Repeat
    DataExporter = Exporter()
    while not stop_event.is_set(): # Be Careful With Stop_Event It Triggers On A Keyboard Shortcut Close!
        # if the wait was interrupted by shutdown, bail before working
        if stop_event.wait(seconds_until(hour_=2, minute_=30)):
            break
        DataExporter.export_all()
        stop_event.wait(1800)



# Create Scheduled Behaviour For: GTFS Downloader
def gtfs_dowloader_scheduler():
    """ Instantiate GTFS Downloader & Start Main Loop """

    # Main Loop Checking If It's 3:30AM, Sleep Until Then, Then Export, The Wait 30 Min, Repeat
    GTFS_Getter = GTFS_Downloader()
    while not stop_event.is_set(): # Be Careful With Stop_Event It Triggers On A Keyboard Shortcut Close!
        # if the wait was interrupted by shutdown, bail before working
        if stop_event.wait(seconds_until(hour_=3, minute_=30)):
            break
        GTFS_Getter.gather_GTFS()
        stop_event.wait(1800)



# Create Scheduled Behaviour For: GTFS Downloader
def data_vizualizer_scheduler():
    """ Create Graphics For Data Pulled & Analyzed """

    # Main Loop Checking If It's 4:30AM, Sleep Until Then, Then Export, The Wait 30 Min, Repeat
    DataViz = Visualizer()
    while not stop_event.is_set(): # Be Careful With Stop_Event It Triggers On A Keyboard Shortcut Close!
        # if the wait was interrupted by shutdown, bail before working
        if stop_event.wait(seconds_until(hour_=4, minute_=30)):
            break
        DataViz.visualize_all()
        stop_event.wait(1800)



# Create Scheduled Behaviour For: GTFS Downloader
def dropbox_uploader_scheduler():
    """ Upload Graphcis & Files To Dropbox """

    # Main Loop Checking If It's 6:30AM, Sleep Until Then, Then Export, The Wait 30 Min, Repeat
    DBX_Uploader = DropBoxUploader()
    while not stop_event.is_set(): # Be Careful With Stop_Event It Triggers On A Keyboard Shortcut Close!
        # if the wait was interrupted by shutdown, bail before working
        if stop_event.wait(seconds_until(hour_=5, minute_=30)):
            break
        DBX_Uploader.upload_all()
        stop_event.wait(1800)



# ----------------------------------------------------------------------------------------------------------------------
# The Main Function Will Run Each Sub Function As It's Own Process & Have Error Catching For Graceful Shut Down
def main():

    # Step #1 Prepare Folders With Environment Setup
    EnvSetup = EnvConfig()
    EnvSetup.setup()

    # Define Each Process, They Should Be Their Own Thread And Run Independently
    threads = [threading.Thread(target = data_collector_scheduler,   name="DataCollector",   daemon=True),
               threading.Thread(target = data_exporter_scheduler,    name="DataExporter",    daemon=True),
               threading.Thread(target = gtfs_dowloader_scheduler,   name="GTFSDownloader",  daemon=True),
               threading.Thread(target = data_vizualizer_scheduler,  name="DataVizualizer",  daemon=True),
               threading.Thread(target = dropbox_uploader_scheduler, name="DropBoxUploader", daemon=True),]
 
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
