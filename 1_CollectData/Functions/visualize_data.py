# Name:                                            Renacin Matadeen
# Date:                                               02/13/2024
# Title                         Main functions used within data collection to visualize data
#
# ----------------------------------------------------------------------------------------------------------------------
import os
import time
import sqlite3
import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import matplotlib.gridspec as grid_spec
# ----------------------------------------------------------------------------------------------------------------------



# Needed Variables For All Functions
td_l_dt_dsply_frmt = "%d-%m-%Y %H:%M:%S"
td_s_dt_dsply_frmt = "%d-%m-%Y"


def __d1_s1(dtfrm):
    """ Data Visualization #1, Step #1 - Format Date To INTs For Easier Grouping By Interval"""

    # For Sanity Of Copy
    df = dtfrm.copy()

    # Format Data To Ints, DT Accessor Took Too Long (50 Sec Before! Now Down To 10 Sec)
    df = df.drop_duplicates(subset=['u_id'])
    df[["YEAR", "MONTH", "DAY"]] = df["dt_colc"].str[:10].str.split('-', expand=True)
    df[["HOUR", "MINUTE", "SECOND"]] = df["dt_colc"].str[11:19].str.split(':', expand=True)
    df.drop(["u_id", "dt_colc", "SECOND"], axis=1, inplace=True)
    df["SECOND"] = "00"
    df["MINUTE"] = df["MINUTE"].astype(int).round(-1).astype(str).str.zfill(2)

    # Can't Have 60, Remove 1 Second In Those Cases
    df.loc[df["MINUTE"] == "60", "SECOND"] = "59"
    df.loc[df["MINUTE"] == "60", "MINUTE"] = "59"

    # Create A New Datetime Timestamp
    df["DT_COL"] = df['YEAR'] + "-" + df['MONTH'] + "-" + df['DAY'] + " " + df['HOUR'] + ":" + df['MINUTE'] + ":" + df['SECOND']
    df.drop(['YEAR', 'MONTH', 'DAY'], axis=1, inplace=True)

    return df


def __d1_s2(dtfrm):
    """ Data Visualization #1, Step #1 - Format Date To INTs For Easier Grouping By Interval"""

    # For Sanity Of Copy
    df = dtfrm.copy()

    # Group By
    def num_unique(x): return len(x.unique())
    grped_time = df.groupby(['DT_COL', 'HOUR', 'MINUTE', 'SECOND'], as_index=False).agg(
                            COUNT_BUS = ("vehicle_id", num_unique)
                            )
    grped_time["DT_COL"] = pd.to_datetime(grped_time["DT_COL"], format=td_l_dt_dsply_frmt)
    grped_time["WK_NUM"] = grped_time["DT_COL"].dt.dayofweek

    # # Convert Hour, Minute, And Seconds To Seconds After 0 (12AM), Remove Unneded Data
    # grped_time["SEC_FTR_12"] = grped_time["HOUR"].astype(int) * 3600 + grped_time["MINUTE"].astype(int) * 60 + grped_time["SECOND"].astype(int)
    # grped_time.drop(['HOUR', 'MINUTE', 'SECOND'], axis=1, inplace=True)
    #
    # # Interate Through Each Day In Dataset
    # grped_time["STR_DT_COL"] = grped_time["DT_COL"].dt.strftime(td_s_dt_dsply_frmt)
    # dates_in = grped_time["DT_COL"].dt.strftime('%Y-%m-%d').unique()
    #
    # return grped_time, dates_in



def data_viz_1(graphics_path, out_path, fl_data, td_dt_mx):
    """
    When called this function will create a scatterplot showing the number of
    unique buses at 10 minute intervals for the entire day. Data is split between
    weekday, and weekend schedules - if data appropriate data was collected.
    """

    # Make Sure We Have At Least 2 Days Worth Of Data
    if len(fl_data["FILE_NAME"].tolist()) >= 2:

        # Find Days Between Today And Minus 2 Days
        fl_data = fl_data[fl_data["DATE"] >= td_dt_mx]
        df = pd.concat([pd.read_csv(path_, usecols=['u_id', 'dt_colc', 'vehicle_id']) for path_ in [f"{out_path}/{x}" for x in fl_data["FILE_NAME"].tolist()]])

        # Data Formatting
        df = __d1_s1(df)

        # Data Grouping
        # grped_time, dates_in =
        __d1_s2(df)
        # del df
        #
        # # Visualize Data Basics For Plot Frame | Define Plot Size 3:2
        # fig, ax = plt.subplots(figsize=(13, 7))
        #
        # # Only Want Data Between [1:-2], As It's Most Likely To Be Complete Days Picture
        # grped_time = grped_time[grped_time["STR_DT_COL"].isin(dates_in[1:-1])]
        #
        # # Create Dataframes For Weekday & Weekend
        # wk_day = grped_time[grped_time["WK_NUM"] <= 4].sort_values(by=['SEC_FTR_12'])
        # wk_end = grped_time[grped_time["WK_NUM"]  > 4].sort_values(by=['SEC_FTR_12'])
        #
        # # If Weekend Is Empty
        # if wk_end.empty:
        #
        #     # Fit Curve To Data | Weekday
        #     curve = np.polyfit(wk_day["SEC_FTR_12"], wk_day["COUNT_BUS"], 15)
        #     poly = np.poly1d(curve)
        #     yy = poly(wk_day["SEC_FTR_12"])
        #
        #     ax.scatter(wk_day["SEC_FTR_12"], wk_day["COUNT_BUS"], marker ="+", c="grey", alpha=0.5, label='# Weekday Buses')
        #     ax.plot(wk_day["SEC_FTR_12"], yy, c="red", alpha=0.5, label='Line Best Fit: Weekday')
        #
        #
        # # If Weekend Is Not Empty
        # else:
        #
        #     # Fit Curve To Data | Weekday
        #     curve = np.polyfit(wk_day["SEC_FTR_12"], wk_day["COUNT_BUS"], 15)
        #     poly = np.poly1d(curve)
        #     yy = poly(wk_day["SEC_FTR_12"])
        #
        #     ax.scatter(wk_day["SEC_FTR_12"], wk_day["COUNT_BUS"], marker ="+", c="grey", alpha=0.5, label='# Weekday Buses')
        #     ax.plot(wk_day["SEC_FTR_12"], yy, c="red", alpha=0.5, label='Line Best Fit: Weekday')
        #
        #     # Fit Curve To Data | Weekend
        #     curve = np.polyfit(wk_end["SEC_FTR_12"], wk_end["COUNT_BUS"], 15)
        #     poly = np.poly1d(curve)
        #     yy = poly(wk_end["SEC_FTR_12"])
        #
        #     ax.scatter(wk_end["SEC_FTR_12"], wk_end["COUNT_BUS"], marker ="x", c="grey", alpha=0.5, label='# Weekend Buses')
        #     ax.plot(wk_end["SEC_FTR_12"], yy, c="blue", alpha=0.5, label='Line Best Fit: Weekend')
        #
        #     ax.legend()
        #
        # # Manually Set X Ticks
        # xlabels = [x for x in range(0, 26, 2)]
        # xticks = [x*3600 for x in xlabels]
        # xlabels = [f"{x}" for x in xlabels]
        # ax.set_xticks(xticks, labels=xlabels)
        #
        # ax.set_xlabel("Time (24 Hour)")
        # ax.set_ylabel("# Of Buses")
        #
        # fig.suptitle('Number Of Brampton Transit Buses Every 10 Minutes')
        # ax.set_title(f"Data Collected Between: {dates_in[1]} & {dates_in[-1]}")
        #
        # # Save The Figure In Graphics Folder
        # fig.savefig(f"{graphics_path}/NumBusesByHour.pdf")



# ----------------------------------------------------------------------------------------------------------------------

def data_viz_2(graphics_path, out_path, fl_data, cur_dt_m2):
    """
    When called this function will create a ridgeline plot of the day before's data.
    """

    # Make Sure We Have At Least 3 Days Worth Of Data
    if len(fl_data["FILE_NAME"].tolist()) >= 3:

        # Find Days Between Today And Minus Only Look At CSVs That Are 3 Days Old Or Newer, Then Read In Data
        fl_data = fl_data[fl_data["DATE"] >= cur_dt_m2]
        df = pd.concat([pd.read_csv(path_, usecols=['u_id', 'dt_colc', 'vehicle_id', 'route_id']) for path_ in [f"{out_path}/{x}" for x in fl_data["FILE_NAME"].tolist()]])
        df = df.drop_duplicates(subset=['u_id'])

        # Find All Data Points That Were Collected Yesterday (Cur Date Minus 1)
        ystrdy = int((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%d'))
        df[["YEAR", "MONTH", "DAY"]] = df["dt_colc"].str[:10].str.split('-', expand=True)
        df[["HOUR", "MINUTE", "SECOND"]] = df["dt_colc"].str[11:19].str.split(':', expand=True)
        df.drop(["u_id", "dt_colc", "SECOND"], axis=1, inplace=True)
        df["SECOND"] = "00"
        df["MINUTE"] = df["MINUTE"].astype(int).round(-1).astype(str).str.zfill(2)

        df.loc[df["MINUTE"] == "60", "SECOND"] = "59"
        df.loc[df["MINUTE"] == "60", "MINUTE"] = "59"

        # Create A New Datetime Timestamp | Keep Data That Was Recorded Yesterday | Delete Unneded Rows
        df["DT_COL"] = df['YEAR'] + "-" + df['MONTH'] + "-" + df['DAY'] + " " + df['HOUR'] + ":" + df['MINUTE'] + ":" + df['SECOND']
        df = df[df["DAY"] == str(ystrdy)].drop_duplicates().drop(['YEAR', 'MONTH', 'DAY'], axis=1)

        # Split Route Number From Columns
        df[["ROUTE", "ID"]] = df["route_id"].str.split('-', expand=True)
        df = df.drop(['ID'], axis=1)

        # Find The Number Of Buses Operating On A Given Route At A Every 10 Time Interval
        def num_ct(x): return len(x)
        grped_time = df.groupby(['ROUTE', 'HOUR', 'MINUTE', 'SECOND', 'DT_COL'], as_index=False).agg(COUNT_BUS = ("ROUTE", num_ct))

        # Create A Column Looking At Seconds Since 12:00AM
        grped_time["SEC_FTR_12"] = grped_time["HOUR"].astype(int)*3600 + grped_time["MINUTE"].astype(int)*60 + grped_time["SECOND"].astype(int)

        # There Is An Issue Where We Have Duplicates
        grped_time["SEC_FTR_12"] = round(grped_time["SEC_FTR_12"], -1)

        # Find The Number Of Routes Operating That Day
        num_routes = [x for x in np.unique(grped_time["ROUTE"])]
        grped_time = grped_time.drop(['HOUR', 'MINUTE', 'SECOND', 'DT_COL'], axis=1)
        grped_time = grped_time.drop_duplicates(subset=["ROUTE", "COUNT_BUS", "SEC_FTR_12"], keep="first")

        # Given Time Intervals Create A Version With The Route Name For Each Route
        all_rows = []
        for rt in num_routes:
            all_rows.extend([f"{rt}_{x}" for x in range(0, 87000, 600)])

        # Create Dataframe That Contains All Timestamps For Each Route
        main_df = pd.DataFrame.from_dict({"RAW_DATA": all_rows})
        main_df[["ROUTE", "SEC_FTR_12"]] = main_df["RAW_DATA"].str.split('_', expand=True)
        main_df = main_df.drop(["RAW_DATA"], axis=1)

        # Convert Columns To Similar Datatypes
        main_df["ROUTE"] = main_df["ROUTE"].astype(str)
        main_df["SEC_FTR_12"] = main_df["SEC_FTR_12"].astype(str)
        grped_time["ROUTE"] = grped_time["ROUTE"].astype(str)
        grped_time["SEC_FTR_12"] = grped_time["SEC_FTR_12"].astype(str)

        # Using The Main Dataframe As A Main Population, Left Join Number Of Buses
        cleaned_data = main_df.merge(grped_time, how="left", on=["ROUTE", "SEC_FTR_12"])
        cleaned_data["COUNT_BUS"] = cleaned_data["COUNT_BUS"].fillna(0)
        cleaned_data["SEC_FTR_12"] = cleaned_data["SEC_FTR_12"].astype(int)


        # What Is The Max Number Of Buses? We Need For Setting Y Limit
        max_num_bus = cleaned_data["COUNT_BUS"].max()

        # Note That We Only Want To Visualize Routes Where The Max Number Of Buses Is Greater Than 1/4 Of The Max
        # We Have Too Much Data And Patterns Are Being Drowned Out
        max_bus_pr_rt = cleaned_data.groupby(['ROUTE'], as_index=False).agg(MAX_BUS = ("COUNT_BUS", "max"))
        max_bus_pr_rt.sort_values(by='MAX_BUS', ascending=False, inplace=True)
        max_bus_pr_rt = max_bus_pr_rt[max_bus_pr_rt["MAX_BUS"] >= 4]

        # Only Look At Data That Is Greater Than Threshold
        cleaned_data = cleaned_data[cleaned_data["ROUTE"].isin(max_bus_pr_rt["ROUTE"])]

        # Define Basics | Grid Should Be 1 Cell Wide & Len(RTS) Long
        yesterday_dt = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
        gs = (grid_spec.GridSpec(len(max_bus_pr_rt["ROUTE"].tolist()), 1))
        doc_len = (len(max_bus_pr_rt["ROUTE"].tolist()) -2)
        fig = plt.figure(figsize=(6, doc_len))
        i = 0
        ax_objs = []
        max_num_bus = cleaned_data["COUNT_BUS"].max() + 1

        # Iterate Through Each Route
        max_rts = len(max_bus_pr_rt["ROUTE"].tolist())
        for idx, rts in enumerate(max_bus_pr_rt["ROUTE"].tolist()):

            # Gather Appropriate Data
            temp_df = cleaned_data.copy()
            temp_df = temp_df[temp_df["ROUTE"] == rts]
            temp_df['RLNG'] = temp_df['COUNT_BUS'].rolling(6).median().round().shift(-3)

            # Create New Axis
            ax = fig.add_subplot(gs[i:i+1, 0:])

            # Fill Axis With Data
            ax.plot(temp_df["SEC_FTR_12"], temp_df["COUNT_BUS"], alpha=1.0, linewidth=0.5, color='black')
            ax.fill_between(temp_df["SEC_FTR_12"], temp_df["COUNT_BUS"], alpha=0.2, color='grey')

            # # Set Route Name For Each Grid
            # ax.text(-0.5, 2 ,f"{rts}", fontsize=8, ha="right", rotation=90)

            # Set Y Axis Limits
            ax.set_ylim([0, max_num_bus])
            ax.set_xlim(left=0, right=87000)
            ax.patch.set_alpha(0)

            # Add A Horizontal Line
            ax.axhline(y=int((max_num_bus/2)), color='grey', linestyle='-', linewidth=0.5, alpha=0.1)

            # If Were Dealing With The Last Grid Object, Do Certain Things
            if (idx + 1) == max_rts:
                ax.set_ylabel(f"{rts}")
                ax.set_yticklabels([])
                ax.set_yticks([])
                xlabels = [x for x in range(0, 26, 2)]
                ax.set_xticks([x*3600 for x in xlabels], [f"{x}" for x in xlabels])
                ax.grid(linestyle='dotted', linewidth=0.5, alpha=0.3)
                plt.xlabel('Time (24 Hour)', style='italic')

            # If Were Dealing With The First Grid Object, Do Certain Things
            elif (idx + 1) == 1:
                ax.set_ylabel(f"{rts}")
                ax.set_xticklabels([])
                ax.set_xticks([])
                ax.set_yticklabels([])
                ax.set_yticks([])
                xlabels = [x for x in range(0, 26, 2)]
                ax.set_xticks([x*3600 for x in xlabels])
                ax.grid(linestyle='dotted', linewidth=0.5, alpha=0.3)
                ax.tick_params(width=0, length=0)
                plt.title(f"Number Of Buses Operating / Route - {yesterday_dt}")

            # If Were Dealing With Any Other Grid Object, Do Certain Things
            else:
                ax.set_ylabel(f"{rts}")
                ax.set_xticklabels([])
                ax.set_xticks([])
                ax.set_yticklabels([])
                ax.set_yticks([])
                xlabels = [x for x in range(0, 26, 2)]
                ax.set_xticks([x*3600 for x in xlabels])
                ax.grid(linestyle='dotted', linewidth=0.5, alpha=0.3)
                ax.tick_params(width=0, length=0)

            # Remove All Splines
            for dir in ["top", "bottom", "left", "right"]:
                ax.spines[dir].set_visible(False)

            # Iterate To Next Roue & Index
            i += 1

        # Plot Data
        gs.update(hspace=0) # For Additional Formatting Or If You Want Them To
        plt.tight_layout()
        fig.savefig(f"{graphics_path}/NumBusesByHourByRoute.pdf")
