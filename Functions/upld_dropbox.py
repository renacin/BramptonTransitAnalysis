def upld_2_dbx(self):
        """
        When called, this function will upload all graphics found in the graphics
        folder and upload them to the connected dropbox application folder.
        """

        try:
            # Navigate To Shell Script Location, And Generate A New Token
            raw_resp = subprocess.check_output(['sh', self.rfresh_tkn_path], stderr=subprocess.DEVNULL)
            raw_resp = raw_resp.decode('ascii')
            json_data = json.loads(raw_resp)

            # Create An Instance Of DBX Class
            dbx = dropbox.Dropbox(json_data["access_token"])

            # If Files In Graphics Folder, Then Upload
            if len(os.listdir(self.out_dict["GRAPHICS"])) > 0:
                for file_ in os.listdir(self.out_dict["GRAPHICS"]):
                    out_path = self.out_dict["GRAPHICS"]
                    file_path = f"{out_path}/{file_}"
                    with open(file_path, "rb") as f:
                        file_data = f.read()
                        dbx.files_upload(file_data, f"/{file_}")


            # For Logging | Good
            tm_nw = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            print(f"{tm_nw}: Success, Uploaded Graphics To DropBox Folder")

        except:
            # For Logging | Bad
            tm_nw = datetime.now().strftime(self.td_l_dt_dsply_frmt)
            print(f"{tm_nw}: Failure, Could Not Upload Graphics To DropBox Folder")