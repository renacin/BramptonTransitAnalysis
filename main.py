# Name:                                            Renacin Matadeen
# Date:                                               06/14/2026
# Title                              Main Logic Of Scheduler & Main Entry Point Of Code
#
# ----------------------------------------------------------------------------------------------------------------------
import time
import threading
from datetime import datetime, timedelta

from Functions.env_config         import Config
from Functions.env_setup          import *
from Functions.gtfs_downloader    import *
from Functions.data_helper        import *
from Functions.data_exporter      import *
from Functions.data_collect       import *
from Functions.data_visualiser    import *
from Functions.upld_dropbox       import *
from Functions.data_deleter       import *

# Keyboard Shortcut Can Trigger This - Be Careful!
stop_event = threading.Event()
# ----------------------------------------------------------------------------------------------------------------------


# Find Out How Many Seconds Until The Export Window | Needed For Scheduling
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



# Create A Nightly Jobs List For Jobs That Only Run Once A Day
NIGHTLY_JOBS = [("GTFS Checker",       GTFS_Downloader,     "gather_GTFS",     2, 30), 
                ("Data Exporter",      Exporter,            "export_all",      3, 00), 
                ("Data Visualiser",    Visualizer,          "visualize_all",   3, 30), 
                ("Dropbox Uploader",   DropBoxUploader,     "upload_all",      4, 00), 
                ("Data Deleter",       Deleter,             "delete_all",      4, 30)
                ]



# Create Logic For Sub-Process That Only Run Once A Day
def job_scheduler(name, worker_cls, method_name, hour_, minute_):
    """ Given parameters (class, function, time), this scheduler will spin up a daemon and assign the job """

    # Need Config Files
    cfg = Config()

    # With The Passed Class, We Call / Assign it To A Variable, Using getattr we get the method (with the name provided) and assign it to a variable
    worker = worker_cls()
    job    = getattr(worker, method_name)

    # Make Sure The Stop Event Isn't True Before Running In A Loop
    while not stop_event.is_set():
        if stop_event.wait(seconds_until(hour_ = hour_, minute_ = minute_)):
            break

        # Try The Job
        try:
            job()

        # Capture The Error
        except Exception as e:
            shared_logger(name, f"Sheduler Issue: {e}", 3, cfg.dblog_path)

        # Wait After Job Is Done
        stop_event.wait(1800)



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




# ----------------------------------------------------------------------------------------------------------------------
# The Main Function Will Run Each Sub Function As It's Own Thread & Have Error Catching For Graceful Shut Down
def main():

    # Step #1 Prepare Folders With Environment Setup
    EnvSetup = EnvConfig()
    EnvSetup.setup()

    # Define Each Thread, Start With Data Collector Separatly First, Then Add Rest
    threads      = [threading.Thread(target = data_collector_scheduler,   name="DataCollector",   daemon=True)]
    for jobs_ in NIGHTLY_JOBS:
        threads += [threading.Thread(target = job_scheduler, args= jobs_, name=jobs_[0].replace(" ", ""), daemon=True)]
 
    # Start Each Thread
    for t in threads:
        t.start()
 
    # Main Loop Of Thread (Keep Looking For A Kill Signal)
    try:
        while not stop_event.is_set():
            time.sleep(1)
 
    except KeyboardInterrupt:
        stop_event.set()
        for t in threads:
            t.join(timeout=30)
 


# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    main()
