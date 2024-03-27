# Name:                                            Renacin Matadeen
# Date:                                               03/27/2024
# Title                                      Can I Upload A File To Dropbox?
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import dropbox
import dropbox.files
# ----------------------------------------------------------------------------------------------------------------------

# Define Path To Tkn
dbx_path = r"/Users/renacin/Desktop/Token.txt"

# Read Tkn
with open(dbx_path, "r") as f:
	dbx_token = f.read()

# Create An Instance Of DBX Class
dbx = dropbox.Dropbox(dbx_token)

# Create A Function That Will Upload The
def up_2_dbx():
	file_path = r"/Users/renacin/Desktop/Test_File.txt"
	with open(file_path, "rb") as f:
		file_data = f.read()
		dbx.files_upload(file_data, f"/TestFile.txt")

# Run Function
up_2_dbx()
