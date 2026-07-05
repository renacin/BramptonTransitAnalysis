# Name:                                            Renacin Matadeen
# Date:                                               07/03/2026
# Title                                      Main Logic Of Dropbox Data Uploader
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import sys
import sqlite3
import pandas as pd
import time as time
from datetime import datetime, timedelta

from Functions.env_config  import Config
from Functions.data_helper import shared_logger
# ----------------------------------------------------------------------------------------------------------------------


class DropBoxUploader():
    """ This class will upload all graphics to a dropbox folder  """

    # -------------------- Functions Run On Instantiation ----------------------
    def __init__(self):
        """ On Instantiation Pull Config Settings """
        # Grab Config Files
        self.cfg = Config()



    # -------------------- Public Function #1 ---------------------------------
    def upload_all(self):
        """
        When Called This Function Will Visualize All Charts.
        """

        # Make Sure DropBox API Is Installed
        try:
            import dropbox
            DROPBOX_IMPORT = True
        except ImportError:
            DROPBOX_IMPORT = False


        # Run Private Functions
        if DROPBOX_IMPORT:
            self.__emptyfolder()
            self.__maingraphics()

        else:
            shared_logger("Dropbox Uploader", "Dropbox Not Installed", 2, self.cfg.dblog_path)



    # -------------------- Private Function #0 ---------------------------------
    def __emptyfolder(self):
        """
        When Called This Function Will Delete Everything In The Dropbox Connected App
        """

        try:
            # Import Dropbox
            import dropbox
            from dropbox.exceptions import ApiError

            # Get Needed Keys From CSV File
            df = pd.read_csv(self.cfg.drpbx_keys)
            Appkey = df.loc[df['Name']       == 'Appkey',       'Value'].item().strip()
            Appsecret = df.loc[df['Name']    == 'Appsecret',    'Value'].item().strip()
            RefreshToken = df.loc[df['Name'] == 'RefreshToken', 'Value'].item().strip()

            # Create An Instance Of The DropBox Connection
            dbx = dropbox.Dropbox(app_key=Appkey, app_secret=Appsecret, oauth2_refresh_token=RefreshToken)

            # From Main App Folder - Delete Everything In Folder
            result = dbx.files_list_folder("")
            for entry in result.entries:
                dbx.files_delete_v2(entry.path_lower)


        except dropbox.exceptions.AuthError as e:
            shared_logger("Dropbox Uploader", f"Deleting Files, Bad Credentials: {e}", 2, self.cfg.dblog_path)

        except dropbox.exceptions.RateLimitError as e:
            shared_logger("Dropbox Uploader", f"Deleting Files, Rate Limit: {e}", 2, self.cfg.dblog_path)

        except dropbox.exceptions.ApiError as e:
            shared_logger("Dropbox Uploader", f"Deleting Files, Not Enough Space: {e}", 2, self.cfg.dblog_path)

        except (ConnectionError, dropbox.exceptions.HttpError) as e:
            shared_logger("Dropbox Uploader", f"Deleting Files, HTTP Error: {e}", 2, self.cfg.dblog_path)



    # -------------------- Private Function #1 ---------------------------------
    def __maingraphics(self):
        """
        When Called This Function Will Upload The Most Recent Graphics In The Graphics Folder
        """

        try:
            # Import Dropbox
            import dropbox
            from dropbox.exceptions import ApiError

            # Get Needed Keys From CSV File
            df = pd.read_csv(self.cfg.drpbx_keys)
            Appkey = df.loc[df['Name']       == 'Appkey',       'Value'].item().strip()
            Appsecret = df.loc[df['Name']    == 'Appsecret',    'Value'].item().strip()
            RefreshToken = df.loc[df['Name'] == 'RefreshToken', 'Value'].item().strip()

            # Create An Instance Of The DropBox Connection
            dbx = dropbox.Dropbox(app_key=Appkey, app_secret=Appsecret, oauth2_refresh_token=RefreshToken)

            # If 1+ Graphics Folders Generated & Files In Grpahics Folder
            all_folders = [f for f in os.listdir(self.cfg.out_graphics_path)]
            if len(all_folders) > 0:

                # Find Max Folder
                recent_folder = max(all_folders)
                recent_folder = os.path.join(self.cfg.out_graphics_path, recent_folder)

                # If Files In Graphics Folder, Then Upload
                if len(os.listdir(recent_folder)) > 0:
                    for file_ in os.listdir(recent_folder):
                        file_path = os.path.join(recent_folder, file_)

                        # Upload File
                        with open(file_path, "rb") as f:
                            dbx.files_upload(f.read(), f"/{file_}", mode=dropbox.files.WriteMode("overwrite"))


        except dropbox.exceptions.AuthError as e:
            shared_logger("Dropbox Uploader", f"Uploading Files, Bad Credentials: {e}", 2, self.cfg.dblog_path)

        except dropbox.exceptions.RateLimitError as e:
            shared_logger("Dropbox Uploader", f"Uploading Files, Rate Limit: {e}", 2, self.cfg.dblog_path)

        except dropbox.exceptions.ApiError as e:
            shared_logger("Dropbox Uploader", f"Uploading Files, Not Enough Space: {e}", 2, self.cfg.dblog_path)

        except (ConnectionError, dropbox.exceptions.HttpError) as e:
            shared_logger("Dropbox Uploader", f"Uploading Files, HTTP Error: {e}", 2, self.cfg.dblog_path)




# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":
    pass

