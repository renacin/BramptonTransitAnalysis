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
            self.__maingraphics()

        else:
            shared_logger("Dropbox Uploader", "Dropbox Not Installed", 2, self.cfg.dblog_path)




    # -------------------- Private Function #1 ---------------------------------
    def __maingraphics(self):
        """
        When Called This Function Upload The Most Recent Graphics In The Graphics Folder
        """

        try:
            # Import Dropbox
            import dropbox

            # Get Needed Keys From CSV File
            df = pd.read_csv(self.cfg.drpbx_keys)
            Appkey = df.loc[df['Name']       == 'Appkey',       'Value'].item().strip()
            Appsecret = df.loc[df['Name']    == 'Appsecret',    'Value'].item().strip()
            RefreshToken = df.loc[df['Name'] == 'RefreshToken', 'Value'].item().strip()

            dbx = dropbox.Dropbox(app_key=Appkey, app_secret=Appsecret, oauth2_refresh_token=RefreshToken)

            print(repr(Appkey), repr(Appsecret), repr(RefreshToken))
            print(dbx.users_get_current_account())



            """ 
            except dropbox.exceptions.AuthError:
                print("Invalid credentials")
                except dropbox.exceptions.AuthError:
                # bad/expired credentials
            except dropbox.exceptions.ApiError as e:
                # the request reached Dropbox but was rejected —
                # file too big, bad path, insufficient space, etc.
            except dropbox.exceptions.RateLimitError:
                # too many requests, back off and retry
            except (ConnectionError, dropbox.exceptions.HttpError):
            """



            # # Navigate To Shell Script Location, And Generate A New Token
            # raw_resp = subprocess.check_output(['sh', self.rfresh_tkn_path], stderr=subprocess.DEVNULL)
            # raw_resp = raw_resp.decode('ascii')
            # json_data = json.loads(raw_resp)

            # # Create An Instance Of DBX Class
            # dbx = dropbox.Dropbox(json_data["access_token"])

            # # If Files In Graphics Folder, Then Upload
            # if len(os.listdir(self.out_dict["GRAPHICS"])) > 0:
            #     for file_ in os.listdir(self.out_dict["GRAPHICS"]):
            #         out_path = self.out_dict["GRAPHICS"]
            #         file_path = f"{out_path}/{file_}"
            #         with open(file_path, "rb") as f:
            #             file_data = f.read()
            #             dbx.files_upload(file_data, f"/{file_}")


            # # For Logging | Good
            # tm_nw = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            # print(f"{tm_nw}: Success, Uploaded Graphics To DropBox Folder")

        except Exception as e:
            print(e)

            # # For Logging | Bad
            # tm_nw = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            # print(f"{tm_nw}: Failure, Could Not Upload Graphics To DropBox Folder")









# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code (For Testing!)
if __name__ == "__main__":

    # Create An Instance For Testing
    DBX_Uploader = DropBoxUploader()
    DBX_Uploader.upload_all()

