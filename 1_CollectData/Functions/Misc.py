
    # ---------------- Private Function #1 For Public Function 5 ---------------
    def __frmt_data_s1(self, data_f, td_dt_mx):
        """ In step 1 of cleaning the main bus data, we must first convert variables
        to an appropriate datatype (to save memory usage) and then determine the
        previous and next bus stop that a bus might have visited. """

        # Sanitize Input
        df = data_f.copy()

        # Collect Garbage
        del data_f
        gc.collect()

        # Format Data To Ints, DT Accessor Took Too Long
        df = df.drop_duplicates(subset=['u_id'])
        df[["YEAR", "MONTH", "DAY"]] = df["dt_colc"].str[:10].str.split('-', expand=True)
        df[["HOUR", "MINUTE", "SECOND"]] = df["dt_colc"].str[11:19].str.split(':', expand=True)


        # We Only Want Data From td_dt_mx Date
        f_day = str(td_dt_mx.day).zfill(2)
        df = df[df["DAY"] == f_day]


        df = df.drop(["YEAR", "MONTH", "DAY", "MINUTE", "SECOND", "u_id"], axis=1)
        df.rename(columns = {"timestamp" :  "EP_TIME",
                             "route_id"  :  "ROUTE_ID",
                             "trip_id"   :  "TRIP_ID",
                             "vehicle_id":  "V_ID",
                             "bearing"   :  "DIR",
                             "latitude"  :  "C_LAT",
                             "longitude" :  "C_LONG",
                             "stop_id"   :  "NXT_STP_ID",
                             "dt_colc"   :  "DATE_TM"}, inplace=True)

        # Try To Reduce Memory Usage
        df["C_LAT"] = df["C_LAT"].astype(np.float32)
        df["C_LONG"] = df["C_LONG"].astype(np.float32)
        df["NXT_STP_ID"] = df["NXT_STP_ID"].astype("Int32")
        df["EP_TIME"] = df["EP_TIME"].astype("Int32")
        df["V_ID"] = df["V_ID"].astype("Int16")
        df["DIR"] = df["DIR"].astype("Int16")
        df["TRIP_ID"] = df["TRIP_ID"].astype("category")
        df["ROUTE_ID"] = df["ROUTE_ID"].astype("category")
        df["HOUR"] = df["HOUR"].astype("category")


        # We Need To Determine Average DIR For Each Trip
        avg_dir = df[["TRIP_ID", "DIR"]].copy()
        avg_dir = avg_dir.groupby(["TRIP_ID"], as_index=False).agg(AVG_DIR = ("DIR", "mean"))
        avg_dir["AVG_DIR"] = avg_dir["AVG_DIR"].astype(int)
        df = df.merge(avg_dir, how="left", on=["TRIP_ID"])
        df.sort_values(["TRIP_ID", "EP_TIME"], inplace=True)

        # Reduce Memory Usage Of New Fields Created
        df["AVG_DIR"] = df["AVG_DIR"].astype("Int16")

        # Collect Garbage
        del avg_dir
        gc.collect()

        # For Each Stop Entry, We Need To Know The Previous Lat, Long, And STP_ID,
        for col in [("P_LAT", "C_LAT"), ("P_LONG", "C_LONG"), ("PRV_STP_ID", "NXT_STP_ID")]:
            df[col[0]] = df.groupby(['ROUTE_ID', 'TRIP_ID', 'AVG_DIR'])[col[1]].shift(1)
            df[col[0]].fillna(df[col[1]], inplace=True)

        # Find Bus Stop Data
        for file in os.listdir(self.out_dict["BUS_STP"]):
            if "BUS_STP_DATA" in file:
                file_path = f'{self.out_dict["BUS_STP"]}/{file}'

        # Read In Bus Stop Data
        needed_cols = ["CLEANED_STOP_NAME_", "CLEANED_STOP_LAT_", "CLEANED_STOP_LON_", "stop_id"]
        bus_stops = pd.read_csv(file_path, usecols=needed_cols)

        # Reduce Size Of Columns
        bus_stops["CLEANED_STOP_LAT_"] = bus_stops["CLEANED_STOP_LAT_"].astype(np.float32)
        bus_stops["CLEANED_STOP_LON_"] = bus_stops["CLEANED_STOP_LON_"].astype(np.float32)
        bus_stops["stop_id"] = bus_stops["stop_id"].astype("Int32")

        # Create Unique Identifier, And Merge Bus Stop Information Onto Main Table
        df["U_NAME"] = df["ROUTE_ID"].astype(str) + "_" + df["TRIP_ID"].astype(str) + "_" + df["AVG_DIR"].astype(str)
        df = df.merge(bus_stops, how="left", left_on=["NXT_STP_ID"], right_on=["stop_id"]).rename(columns = {"CLEANED_STOP_NAME_": "NXT_STP_NAME",
                                                                                                             "CLEANED_STOP_LAT_": "NXT_STP_LAT",
                                                                                                             "CLEANED_STOP_LON_": "NXT_STP_LONG"
                                                                                                             }).drop(["stop_id"], axis=1)

        df = df.merge(bus_stops, how="left", left_on=["PRV_STP_ID"], right_on=["stop_id"]).rename(columns = {"CLEANED_STOP_NAME_": "PRV_STP_NAME",
                                                                                                             "CLEANED_STOP_LAT_": "PRV_STP_LAT",
                                                                                                             "CLEANED_STOP_LON_": "PRV_STP_LONG"
                                                                                                             }).drop(["stop_id"], axis=1)

        # Collect Garbage
        del bus_stops
        gc.collect()

        # Final Bits Of Formatting
        df.sort_values(["ROUTE_ID", "TRIP_ID", "EP_TIME"], inplace=True)
        df.drop_duplicates(inplace=True)

        # For Logging
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Data Formatting Step #1 - Complete")

        return df



    # ---------------- Private Function #2 For Public Function 5 ---------------
    def __frmt_data_s2(self, data_f, td_dt_mx):
        """ In step 2 of cleaning the main bus data, remove unneccesary rows within a
        trip, we only need the row closest to a given bus stop in a trip - this reduces
        the amount of data recording idling time. Once complete we then calculate the
        speed of the bus, given only the observations from that trip (no inferences). Once
        we have the speed of the bus, we determine the arrival time to the next bus stop. """

        # Sanitize Input
        data_pull = data_f.copy()
        del data_f
        gc.collect()

        # Remove Entries Where Bus Is Idling, Or Has Kept Transponder Running After The First Occurence At The Last Stop | Append All Dta To New Dataframe
        gb = data_pull.groupby("U_NAME")
        transit_df = pd.concat([x[1].loc[x[1]["NXT_STP_NAME"].where(x[1]["NXT_STP_NAME"]==x[1]["NXT_STP_NAME"].iloc[0]).last_valid_index():x[1]["PRV_STP_NAME"].where(x[1]["PRV_STP_NAME"]==x[1]["PRV_STP_NAME"].iloc[-1]).first_valid_index()] for x in gb])
        del data_pull, gb
        gc.collect()


        # Calculate Distance Between Current Location & Previous Location | Create A Dataframe Elaborating Distance Traveled & Speed
        transit_df["DST_BTW_LOCS"] = vec_haversine((transit_df["P_LAT"].values, transit_df["P_LONG"].values), (transit_df["C_LAT"].values, transit_df["C_LONG"].values))

        # First Create A Copy Of Main Data Table | We Only Need Certain Columns, Not All!
        speed_df = transit_df[['U_NAME', 'TRIP_ID', 'ROUTE_ID', 'V_ID', 'AVG_DIR', 'EP_TIME', 'HOUR', 'DST_BTW_LOCS']].copy()


        # Find The Previous Travel Time, Determine The Trip Speed & Trip Duration
        speed_df["P_EP_TIME"] = speed_df.groupby(["U_NAME"])["EP_TIME"].shift(+1)
        speed_df.dropna(subset=["P_EP_TIME"], inplace=True)
        speed_df["TRIP_DUR"] = (speed_df["EP_TIME"] - speed_df["P_EP_TIME"]) / 3600
        speed_df["TRIP_SPD"] = speed_df["DST_BTW_LOCS"] / speed_df["TRIP_DUR"]

        # Round & Convert Data Types To Save Memory?
        speed_df["TRIP_DUR"] = speed_df["TRIP_DUR"].astype(np.float16)
        speed_df["TRIP_SPD"] = speed_df["TRIP_SPD"].astype(np.float16)
        gc.collect()

        # Create A Data Frame Elaborating The Average Speed For A Trip, This May Be Useful In The Future Keep It!
        for col in ["ROUTE_ID", "TRIP_ID", "HOUR"]:
            speed_df[col] = speed_df[col].astype("object")
        speed_df = speed_df.groupby(["ROUTE_ID", "TRIP_ID", "AVG_DIR", "V_ID"], as_index=False).agg(TRIP_SPD = ("TRIP_SPD", "mean" ),
                                                                                                    HOUR     = ("HOUR"    , "first")
                                                                                                    )

        # The GroupBy Drops Data Types For Some Reason, Convert Back To Appropriate Datatype
        speed_df["TRIP_ID"] = speed_df["TRIP_ID"].astype("category")
        speed_df["ROUTE_ID"] = speed_df["ROUTE_ID"].astype("category")
        speed_df["AVG_DIR"] = speed_df["AVG_DIR"].astype("Int16")
        speed_df["V_ID"] = speed_df["V_ID"].astype("Int16")

        # Export Speed DF To Folder
        cleaned_dt = f"{td_dt_mx.day:0>2}-{td_dt_mx.month:0>2}-{td_dt_mx.year}"
        dt_string = datetime.now().strftime(self.td_s_dt_dsply_frmt)
        out_path = self.out_dict["BUS_SPEED"] + f"/BUS_SPEED_DATA_{cleaned_dt}.csv"
        speed_df.to_csv(out_path, index=False)

        # If Next Stop Is Equal To Previous Stop, Replace With Blank, Foward Fill Next Stop Values & Replace First
        for n_col, p_col in zip(["NXT_STP_ID", "NXT_STP_NAME", "NXT_STP_LAT", "NXT_STP_LONG"], ["PRV_STP_ID", "PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG"]):
            transit_df.loc[transit_df[n_col] == transit_df[p_col], p_col] = np.nan
            transit_df[p_col] = transit_df.groupby(["ROUTE_ID", "TRIP_ID", "AVG_DIR"])[p_col].ffill()
            transit_df[p_col] = transit_df[p_col].fillna(transit_df[n_col])

        # Only Keep The Last Occurence If Duplicates Are Found
        transit_df = transit_df.drop_duplicates(subset=["ROUTE_ID", "TRIP_ID", "AVG_DIR", "NXT_STP_ID", "PRV_STP_ID"], keep="last")

        # Just Want To Know Te Time The Bus Arrived At It's Next Stop Given Average Speed
        transit_df = transit_df.drop(["HOUR", "P_LAT", "P_LONG", "PRV_STP_ID", "PRV_STP_NAME", "PRV_STP_LAT", "PRV_STP_LONG", "DST_BTW_LOCS"], axis=1)
        transit_df["DTS_2_NXT_STP"] = vec_haversine((transit_df["C_LAT"].values, transit_df["C_LONG"].values), (transit_df["NXT_STP_LAT"].values, transit_df["NXT_STP_LONG"].values))
        transit_df["DTS_2_NXT_STP"] = round(transit_df["DTS_2_NXT_STP"], 2)

        # Merge both Tables Together
        transit_df = transit_df.merge(speed_df, how="left", on=["ROUTE_ID", "TRIP_ID", "AVG_DIR", "V_ID"])
        del speed_df
        gc.collect()

        # Calculate Speed
        transit_df["SEC_2_NXT_STP"]  = (transit_df["DTS_2_NXT_STP"] / transit_df["TRIP_SPD"]) * 3600
        transit_df["NXT_STP_ARV_TM"] = transit_df["EP_TIME"] + transit_df["SEC_2_NXT_STP"]
        transit_df["NXT_STP_ARV_TM"] = transit_df["NXT_STP_ARV_TM"].astype(dtype = int, errors = 'ignore')
        transit_df["NXT_STP_ARV_DTTM"] = pd.to_datetime(transit_df["NXT_STP_ARV_TM"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')

        # Remove Zeros From NXT_STP_ARV_TM
        transit_df = transit_df[transit_df["NXT_STP_ARV_TM"] != 0]

        # For Logging
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Data Formatting Step #2 - Complete")

        return transit_df



    # ---------------- Private Function #3 For Public Function 5 ---------------
    def __frmt_data_s3(self, data_f):
        """ In step 3 of cleaning the main bus data, we infer the bus stops in between observed
        bus stops. A bus doesn't always stop at every bus stop, but we still need to have that data.
        Mark data that we have as informed data (closest we have to actual observation). Merge onto
        a list of routes in order, and use ReGex to find the clusters of bus stops that were not
        visited. From there use the information we have to determine when the bus might have passed
        that bus stop. """

        # Sanitize Input
        transit_df = data_f.copy()
        del data_f
        gc.collect()

        # We Only Need Certain Bits Of Data, Also For The Data That We Have Mark It As IE (Informed Estimation)
        transit_df["DATA_TYPE"] = "IE"
        transit_df = transit_df[['TRIP_ID', 'ROUTE_ID',
                                 'V_ID', 'NXT_STP_NAME',
                                 'NXT_STP_ARV_TM', 'NXT_STP_ARV_DTTM',
                                 'NXT_STP_ID', 'DATA_TYPE']].rename(columns={'NXT_STP_NAME': "STP_NM",
                                                                             'NXT_STP_ARV_TM': "STP_ARV_TM",
                                                                             'NXT_STP_ARV_DTTM': "STP_ARV_DTTM",
                                                                             'NXT_STP_ID': "STP_ID"})

        # We Need To Join The Trip Data To The Bus Stops (In Order) For A Given Route
        trips_obs = transit_df.groupby(["TRIP_ID"], as_index=False).agg(TRIP_ID = ("TRIP_ID", "first"),
                                                                        RT_ID   = ("ROUTE_ID", "first")
                                                                        )

        trips_obs["RT_ID"] = trips_obs["RT_ID"].str.split("-").str[0]
        trips_obs["RT_ID"] = trips_obs["RT_ID"].astype(dtype = int, errors = 'ignore')
        trips_obs["RT_ID"] = trips_obs["RT_ID"].astype("Int16")

        # Read In Routes Data, Only Look At Needed Data, Convert To Smaller Data Type
        for file in os.listdir(self.out_dict["BUS_STP"]):
            if "BUS_RTE_DATA" in file:
                stp_file_path = f'{self.out_dict["BUS_STP"]}/{file}'

        needed_cols = ['RT_ID', 'RT_NAME',
                       'RT_VER', 'STP_NM',
                       'RT_ID_VER', 'RT_DIR',
                       'RT_GRP', 'RT_GRP_NUM']

        bus_routes = pd.read_csv(stp_file_path, usecols = needed_cols)

        # Before Using Bus Routes, We Need To Make Sure The Data Contains No Duplicates As Identified In Previous Attemps
        bus_routes["U_ID"] = bus_routes["RT_ID"].astype(str) + "_" + bus_routes["RT_NAME"].astype(str) + "_" + bus_routes["RT_VER"].astype(str)
        bus_routes = bus_routes.drop_duplicates()

        # Recreate Stop Num & Total Number Of Stops
        bus_routes["RT_STP_NUM"] = bus_routes.groupby(["U_ID"]).cumcount() + 1
        num_stps_df = bus_routes.groupby("U_ID", as_index=False).agg(RT_NUM_STPS = ("U_ID", "count"))
        bus_routes = bus_routes.merge(num_stps_df, on='U_ID', how='left')
        del num_stps_df, bus_routes["U_ID"]
        gc.collect()

        # Only Keep Data That Is In Scope
        bus_routes = bus_routes[bus_routes["RT_ID"].isin(trips_obs["RT_ID"])]

        # Try To Reduce Memory Usage
        bus_routes["RT_ID"]       = bus_routes["RT_ID"].astype("Int16")
        bus_routes["RT_VER"]      = bus_routes["RT_VER"].astype("Int16")
        bus_routes["RT_STP_NUM"]  = bus_routes["RT_STP_NUM"].astype("Int16")
        bus_routes["RT_NUM_STPS"] = bus_routes["RT_NUM_STPS"].astype("Int16")
        bus_routes["RT_GRP_NUM"]  = bus_routes["RT_GRP_NUM"].astype("Int16")

        # Left Join Bus Route Data Onto Unique Trip Data Using RT_ID As A Key
        trips_obs = trips_obs.merge(bus_routes, how="left", on=["RT_ID"])
        trips_obs = trips_obs.merge(transit_df, how="left", on=["TRIP_ID", "STP_NM"])
        del transit_df, bus_routes
        gc.collect()

        # Create A Column That Identifies The Trip ID, RT_ID, And RT_VER, Remove U_IDs That Have No Data In Them
        trips_obs["U_ID"] = trips_obs["TRIP_ID"].astype(str) + "_" + trips_obs["RT_ID"].astype(str) + "_" + trips_obs["RT_VER"].astype(str)
        count_df = trips_obs.groupby(["U_ID"], as_index=False).agg(U_ID  = ("U_ID", "first"),
                                                                   COUNT = ("DATA_TYPE", "count")
                                                                   )

        count_df[["TRIP_ID", "RT_ID", "RT_VER"]] = count_df["U_ID"].str.split('_', expand=True)
        count_df["COUNT_NAME"] = count_df["TRIP_ID"].astype(str) + "_" + count_df["COUNT"].astype(str)

        # Only Keep Trips That Actually Happened, Remove Eroneous Data
        max_obs = count_df.groupby(["TRIP_ID", "RT_ID"], as_index=False).agg(COUNT = ("COUNT", "max"))
        max_obs["COUNT_NAME"] = max_obs["TRIP_ID"].astype(str) + "_" + max_obs["COUNT"].astype(str)
        max_obs.drop(["TRIP_ID", "RT_ID", "COUNT"], axis=1, inplace=True)

        # Merge Back To Count DF, Make Sure We Are Looking At Correct U_IDs
        count_df = max_obs.merge(count_df, how="left", on=["COUNT_NAME"])
        trips_obs = trips_obs[trips_obs["U_ID"].isin(count_df["U_ID"])]
        del max_obs, count_df
        gc.collect()


        # Need To Solve The Later Later Stop Earlier Time Issue
        # Make An Indicator For Each Row
        trips_obs["ROW_ID"] = range(len(trips_obs))


        # Make A Copy Of The Database, Keep Only Needed Columns
        test_df = trips_obs[["STP_ARV_TM", "U_ID", "ROW_ID"]].copy()
        test_df = test_df.dropna(subset=["STP_ARV_TM"])


        # If U_ID Not In Test_DF Don't Keep It In Main DF
        trips_obs = trips_obs[trips_obs["U_ID"].isin(test_df["U_ID"])]

        # We Only Want To Keep Bus Trips With More Than One 1 Observation, Need To Know Next STP Time, It Must Be Bigger
        test_df = pd.concat([x[1] for x in test_df.groupby("U_ID") if len(x[1]) > 1])
        test_df['NXT_STP_ARV_TM'] = test_df.groupby(['U_ID'])['STP_ARV_TM'].shift(-1)
        test_df['TM_DIFF'] = test_df['NXT_STP_ARV_TM'] - test_df['STP_ARV_TM']


        # We Need To Know Which Rows To Remove Only Keep Data Between First Observation Of Negative Value And Everything Else
        gb = test_df.groupby("U_ID")
        data = []
        for x in gb:

            # Get All Time Diffs As List
            td_df_lst = x[1]["TM_DIFF"].tolist()

            # If Negative Value In TM_DIFF Column
            if any(tm_stampt < 0 for tm_stampt in td_df_lst):
                data.append(x[1].loc[x[1]["TM_DIFF"].where(x[1]["TM_DIFF"] < 0).first_valid_index():])

        # Put Everything Together
        test_df = pd.concat(data)
        test_df.drop(["NXT_STP_ARV_TM", "TM_DIFF", "STP_ARV_TM", "U_ID"], axis=1, inplace=True)
        test_df["ERASE_DATA"] = "YES"


        # Merge Onto Main Table, Clean Up
        trips_obs = trips_obs.merge(test_df, how="left", on=["ROW_ID"])
        trips_obs.loc[trips_obs["STP_ARV_TM"].isna(), "ERASE_DATA"] = "YES"
        del test_df
        gc.collect()


        # Convert Column To String, Remove Erroneous Data
        for col in ["ROUTE_ID", "STP_ARV_DTTM", "DATA_TYPE"]:
            trips_obs[col] = trips_obs[col].astype(str)
        for col in ["ROUTE_ID", "STP_ARV_DTTM", "DATA_TYPE"]:
            trips_obs.loc[trips_obs["ERASE_DATA"] == "YES", col] = ""
        for col in ["V_ID", "STP_ARV_TM"]:
            trips_obs.loc[trips_obs["ERASE_DATA"] == "YES", col] = np.nan

        # Remove Unneeded Column
        trips_obs.drop(["ERASE_DATA", "ROW_ID"], axis=1, inplace=True)


        # We Only Want Data Between The First Occurence, And The Last Of A Given Trip, Remove Occurences With No Data
        data = []
        gb = trips_obs.groupby("TRIP_ID")
        for x in gb:

            # If There Is Data In The DATA_TYPE Column, Then Append Later, Else Pass
            if any(d_tp == "IE" for d_tp in x[1]["DATA_TYPE"].to_list()):
                data.append(x[1].loc[x[1]["DATA_TYPE"].where(x[1]["DATA_TYPE"]=="IE").first_valid_index():x[1]["DATA_TYPE"].where(x[1]["DATA_TYPE"]=="IE").last_valid_index()])

        trips_obs = pd.concat(data)
        del gb, data, trips_obs["U_ID"]


        # Drop Duplicates Again, This Time Based On TRIP_ID, And Stop Number
        trips_obs = trips_obs.drop_duplicates(subset=["TRIP_ID", "RT_STP_NUM"])


        # Create An Encoding, For A New Column. If There Is Data In The Timestampt Then 1, Else 0
        trips_obs["DATA_FLG"] = "1"
        trips_obs.loc[trips_obs["STP_ARV_TM"].isna(), "DATA_FLG"] = "0"

        # Create A Temp Row Index, We Will Use This To Reorganize Data
        trips_obs["TEMP_IDX"] = range(len(trips_obs))

        # Find Bus Loc Data
        for file in os.listdir(self.out_dict["BUS_STP"]):
            if "BUS_STP_DATA" in file:
                file_path = f'{self.out_dict["BUS_STP"]}/{file}'

        # Read In Bus Loc Data & Merge To Trips Obs DF
        needed_cols = ['stop_id', 'stop_name', 'CLEANED_STOP_LAT_', 'CLEANED_STOP_LON_']
        bus_locs = pd.read_csv(file_path, usecols = needed_cols)
        bus_locs.rename(columns = {"stop_name": "STP_NM",
                                   "stop_id": "STP_ID",
                                   "CLEANED_STOP_LAT_": "STP_LAT",
                                   "CLEANED_STOP_LON_": "STP_LON"}
                                   ,inplace = True)

        # Split Methodolgies To Merge Data. If We Have Stop ID, Join On Stop ID, If We Don't Have Stop ID, Join On Name, But Keep The First One
        stp_id_merge = trips_obs[trips_obs["DATA_TYPE"] == "IE"]
        stp_id_merge = stp_id_merge.merge(bus_locs[["STP_ID", "STP_LAT", "STP_LON"]].copy(), how="left", on=["STP_ID"])

        stp_nm_merge = trips_obs[trips_obs["DATA_TYPE"] != "IE"]
        stp_nm_merge = stp_nm_merge.merge(bus_locs[["STP_NM", "STP_LAT", "STP_LON"]].copy(), how="left", on=["STP_NM"])
        stp_nm_merge = stp_nm_merge.drop_duplicates(subset=['TRIP_ID', 'RT_STP_NUM'])

        # Merge Data Together
        trips_obs = pd.concat([stp_id_merge, stp_nm_merge])
        trips_obs = trips_obs.sort_values("TEMP_IDX")
        del needed_cols, bus_locs, stp_id_merge, stp_nm_merge, trips_obs["TEMP_IDX"]
        gc.collect()


        # Determine Distance To Next Bus Stop
        trips_obs['NXT_STP_LAT'] = trips_obs['STP_LAT'].shift(-1)
        trips_obs['NXT_STP_LON'] = trips_obs['STP_LON'].shift(-1)
        trips_obs['NXT_STP_NM'] = trips_obs.groupby('TRIP_ID')['STP_NM'].shift(-1)

        trips_obs["DTS_2_NXT_STP"] = vec_haversine((trips_obs["STP_LAT"].values, trips_obs["STP_LON"].values), (trips_obs["NXT_STP_LAT"].values, trips_obs["NXT_STP_LON"].values))
        trips_obs.drop(columns=["STP_LAT", "STP_LON", "NXT_STP_LAT", "NXT_STP_LON"], inplace = True)

        # There Are Situations Where The DTS_2_NXT_STP Is Null, Fill With 0
        trips_obs["DTS_2_NXT_STP"] = trips_obs["DTS_2_NXT_STP"].fillna(0)


        # Iterate Through The Data Looking Patterns, Find Clusters Of Missing Data, Use Regex To Find All Matches
        re_pat = r"(?:1)0{1,100}"

        # Iterate Through Matches & Find Corresponding Pattern In String & Index List, Create An Index That Will Help Identify Order
        trips_obs["IDX_R"] = np.arange(len(trips_obs))
        trips_obs = trips_obs.reset_index()
        df_flag_str = "".join(trips_obs["DATA_FLG"].tolist())
        for cntr, x in enumerate(re.finditer(re_pat, df_flag_str)):

            # Convert To List, & Fix
            grp_mtch_idx = list(x.span())

            # Find The Needed Time Data
            time_data = list(trips_obs.iloc[grp_mtch_idx[0]:grp_mtch_idx[1]+1]["STP_ARV_TM"].to_numpy())

            # Find The Total Duration Of The Trip (Find Time At Begining -1 Of Cluster & Time At Ending +1 Of Cluster)
            total_distance = sum(trips_obs.iloc[grp_mtch_idx[0]:grp_mtch_idx[1]]["DTS_2_NXT_STP"].to_numpy())
            total_time = time_data[-1] - time_data[0]
            total_time = total_time / 3600

            # Determine Average Speed
            c_svg_spd = total_distance / total_time

            # Time Between Last Observation Before Outage, And First Observation After Outage
            time_btw_stops = trips_obs.iloc[grp_mtch_idx[1]]["STP_ARV_TM"] - trips_obs.iloc[grp_mtch_idx[0]]["STP_ARV_TM"]

            # Set Value Between Index As Cluster # ID
            trips_obs.at[grp_mtch_idx[0] + 1:grp_mtch_idx[1] -1, "TRIP_CLUSTER_ID"] = f"C{cntr}"
            trips_obs.at[grp_mtch_idx[0] + 1:grp_mtch_idx[1] -1, "CLUSTER_AVG_SPD"] = c_svg_spd

            # We Need To Know The Timestamp Before The Error Cluster Began
            trips_obs.at[grp_mtch_idx[0] + 1:grp_mtch_idx[1] -1, "ERROR_START_TIME"] = trips_obs.iloc[grp_mtch_idx[0]]["STP_ARV_TM"]


        # Determine The Time It Took Given The Speed And Distance
        trips_obs["SECS_TRVL_DSTNC"] = (trips_obs["DTS_2_NXT_STP"] / trips_obs["CLUSTER_AVG_SPD"]) * 3600
        trips_obs["SECS_TRVL_DSTNC"] = trips_obs["SECS_TRVL_DSTNC"].fillna(0)

        # Make A Copy Of Certain Columns, And Determine The Running Sum Of Time Traveled For Distance, Add Back To Original Time And Merge To Main DF
        cm_sum_df = trips_obs[["IDX_R", "TRIP_CLUSTER_ID", "CLUSTER_AVG_SPD", "ERROR_START_TIME", "SECS_TRVL_DSTNC"]].copy()
        cm_sum_df = cm_sum_df.dropna(subset=["TRIP_CLUSTER_ID"])
        cm_sum_df["TRV_TM_CUMSUM"] = cm_sum_df.groupby(["TRIP_CLUSTER_ID"])["SECS_TRVL_DSTNC"].cumsum()
        cm_sum_df["TRL_ARV_TM_EST"] = cm_sum_df["ERROR_START_TIME"] + cm_sum_df["TRV_TM_CUMSUM"]

        cm_sum_df.drop(columns=["CLUSTER_AVG_SPD", "ERROR_START_TIME", "SECS_TRVL_DSTNC", "TRV_TM_CUMSUM"], inplace = True)
        trips_obs.drop(columns=['CLUSTER_AVG_SPD', 'ERROR_START_TIME', 'DATA_FLG', 'SECS_TRVL_DSTNC'], inplace = True)


        # Merge Data Together
        trips_obs = trips_obs.merge(cm_sum_df, how="left", on=["IDX_R", "TRIP_CLUSTER_ID"])
        trips_obs.drop(columns=["IDX_R", "TRIP_CLUSTER_ID"], inplace = True)
        for col in ["ROUTE_ID", "V_ID"]:
            trips_obs[col] = trips_obs[col].ffill()

        # Make Note Of Type Of Data, CE = Complete Estimation
        trips_obs.loc[trips_obs["STP_ARV_TM"].isna(), "STP_ARV_TM"] = trips_obs["TRL_ARV_TM_EST"]
        trips_obs.loc[trips_obs["DATA_TYPE"] == "", "DATA_TYPE"] = "CE"

        # Remove Unneeded Columns
        trips_obs.drop(columns=["STP_ARV_DTTM", "TRL_ARV_TM_EST"], inplace = True)

        # Forward Fill Data
        trips_obs["STP_ARV_TM"] = round(trips_obs["STP_ARV_TM"], 0)
        trips_obs["STP_ARV_DTTM"] = pd.to_datetime(trips_obs["STP_ARV_TM"], unit='s').dt.tz_localize('UTC').dt.tz_convert('Canada/Eastern')

        trips_obs.loc[trips_obs["ROUTE_ID"] == "", "ROUTE_ID"] = np.nan
        trips_obs["ROUTE_ID"] = trips_obs.groupby(["TRIP_ID"])["ROUTE_ID"].ffill()

        # Round A Few Columns
        for col in ["DTS_2_NXT_STP"]:
            trips_obs[col] = trips_obs[col].round(2)

        # Final Bits Of Formatting, Drop Duplicates, And If No Data For Trip ID Don't Keep
        trips_obs = trips_obs.drop_duplicates()
        gb = trips_obs.groupby("TRIP_ID")
        data = []
        for x in gb:

            # Get All Time Diffs As List
            td_df_lst = x[1]["STP_ARV_TM"].tolist()

            # If All Are Not NaN, And Lenght Is Larger Than 1
            if all(tm_stampt != np.nan for tm_stampt in td_df_lst) and (len(td_df_lst) >= 2):
                data.append(x[1])

        # Put Everything Together
        trips_obs = pd.concat(data)
        del trips_obs["STP_ID"]

        # Drop Duplicates Again, This Time Based On TRIP_ID, And Stop Number
        trips_obs = trips_obs.drop_duplicates(subset=["TRIP_ID", "RT_STP_NUM"])

        # If Last Stop & No Data Remove Erroneous Data From Next Stop
        trips_obs["NXT_STP_NM"] = trips_obs["NXT_STP_NM"].fillna("--")
        trips_obs.loc[trips_obs["NXT_STP_NM"] == "--", "DTS_2_NXT_STP"] = 0

        # For Logging
        now = datetime.now().strftime(self.td_l_dt_dsply_frmt)
        print(f"{now}: Data Formatting Step #3 - Complete")

        return trips_obs



    # ------------------------- Public Function 5 ------------------------------
    def frmt_rwbslc_data(self, td_dt_mx):
        """
        When called, this function will read the bus data collected, and exported
        from the day before, format the data - determining speed, and time when it arrived
        at a given bus stop, and keep only needed entries. The formatted data will then
        be exported as a CSV to an output folder.
        """

        # Step #0: Gather Yesterday's Bus Location Data
        try:

            dir_list = [x for x in os.listdir(self.out_dict["BUS_LOC"]) if ".csv" in x]
            df = pd.DataFrame(dir_list, columns=['FILE_NAME'])

            # Create A Dataframe With The Time The File Was Created & Output
            df["DATE"] = df["FILE_NAME"].str.split('_').str[-1]
            df["DATE"] = df["DATE"].str.replace(".csv", "", regex=False)
            df["DATE"] = pd.to_datetime(df["DATE"], format = self.td_s_dt_dsply_frmt)


            # We Only Need Certain Columns On Data Ingest
            td_dt_mx = datetime.strptime(td_dt_mx, self.td_s_dt_dsply_frmt)
            df = df[df["DATE"] >= td_dt_mx]
            needed_cols = ['u_id', 'timestamp', 'route_id', 'trip_id', 'vehicle_id', 'bearing', 'latitude', 'longitude', 'stop_id', 'dt_colc']
            df = pd.concat([pd.read_csv(path_, usecols = needed_cols) for path_ in [f'{self.out_dict["BUS_LOC"]}/{x}' for x in df["FILE_NAME"].tolist()]])
            del needed_cols

            # Format Data
            trips_obs = self.__frmt_data_s3(self.__frmt_data_s2(self.__frmt_data_s1(df, td_dt_mx), td_dt_mx))

            # Export Speed DF To Folder
            cleaned_dt = f"{td_dt_mx.day:0>2}-{td_dt_mx.month:0>2}-{td_dt_mx.year}"
            dt_string = datetime.now().strftime(self.td_s_dt_dsply_frmt)
            out_path = self.out_dict["FRMTD_DATA"] + f"/FRMTD_DATA_{cleaned_dt}.csv"
            trips_obs.to_csv(out_path, index=False)

        except Exception as e:
            print(e)
            pass
