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
from sklearn.neighbors import KernelDensity
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.gridspec as grid_spec
# ----------------------------------------------------------------------------------------------------------------------


def data_viz_1(graphics_path, out_path, fl_data, td_dt_m6):
    """
    When called this function will create a scaterplot showing the number of
    unique buses at 10 minute intervals for the entire day. Data is split between
    weekday, and weekend schedules - if data appropriate data was collected.
    """

    # Make Sure We Have At Least 5 Days Worth Of Data
    if len(fl_data["FILE_NAME"].tolist()) >= 5:

        # Find Days Between Today And Minus 5 Days
        fl_data = fl_data[fl_data["DATE"] >= td_dt_m6]

        # Ingest All Data Into Pandas Dataframe
        df = pd.concat([pd.read_csv(path_, usecols=['u_id', 'dt_colc', 'vehicle_id']) for path_ in [f"{out_path}/{x}" for x in fl_data["FILE_NAME"].tolist()]])

        # Format Data To Ints, DT Accessor Took Too Long (50 Sec Before! Now Down To 10 Sec)
        df = df.drop_duplicates(subset=['u_id'])
        df[["YEAR", "MONTH", "DAY"]] = df["dt_colc"].str[:10].str.split('-', expand=True)
        df[["HOUR", "MINUTE", "SECOND"]] = df["dt_colc"].str[11:19].str.split(':', expand=True)
        df.drop(["u_id", "dt_colc", "SECOND"], axis=1, inplace=True)
        df["SECOND"] = "00"
        df["MINUTE"] = df["MINUTE"].astype(int).round(-1).astype(str).str.zfill(2)

        df.loc[df["MINUTE"] == "60", "MINUTE"] = "59"
        df.loc[df["MINUTE"] == "60", "SECOND"] = "59"

        # Create A New Datetime Timestamp
        df["DT_COL"] = df['YEAR'] + "-" + df['MONTH'] + "-" + df['DAY'] + " " + df['HOUR'] + ":" + df['MINUTE'] + ":" + df['SECOND']
        df.drop(['YEAR', 'MONTH', 'DAY'], axis=1, inplace=True)

        # Groub By Hour
        def num_unique(x): return len(x.unique())
        grped_time = df.groupby(['DT_COL', 'HOUR', 'MINUTE', 'SECOND'], as_index=False).agg(
                                COUNT_BUS = ("vehicle_id", num_unique)
                                )
        grped_time["DT_COL"] = pd.to_datetime(grped_time["DT_COL"], format='%Y-%m-%d %H:%M:%S')
        grped_time["WK_NUM"] = grped_time["DT_COL"].dt.dayofweek

        del df

        # Convert Hour, Minute, And Seconds To Seconds After 0 (12AM), Remove Unneded Data
        grped_time["SEC_FTR_12"] = grped_time["HOUR"].astype(int)*3600 + grped_time["MINUTE"].astype(int)*60 + grped_time["SECOND"].astype(int)
        grped_time.drop(['HOUR', 'MINUTE', 'SECOND'], axis=1, inplace=True)

        # Interate Through Each Day In Dataset
        grped_time["STR_DT_COL"] = grped_time["DT_COL"].dt.strftime('%Y-%m-%d')
        dates_in = grped_time["DT_COL"].dt.strftime('%Y-%m-%d').unique()

        # Basics For Plot Frame | Define Plot Size 3:2
        fig, ax = plt.subplots(figsize=(13, 7))

        # Only Want Data Between [1:-2], As It's Most Likely To Be Complete Days Picture
        grped_time = grped_time[grped_time["STR_DT_COL"].isin(dates_in[1:-1])]

        # Create Dataframes For Weekday & Weekend
        wk_day = grped_time[grped_time["WK_NUM"] <= 4].sort_values(by=['SEC_FTR_12'])
        wk_end = grped_time[grped_time["WK_NUM"]  > 4].sort_values(by=['SEC_FTR_12'])

        # If Weekend Is Empty
        if wk_end.empty:

            # Fit Curve To Data | Weekday
            curve = np.polyfit(wk_day["SEC_FTR_12"], wk_day["COUNT_BUS"], 15)
            poly = np.poly1d(curve)
            yy = poly(wk_day["SEC_FTR_12"])

            ax.scatter(wk_day["SEC_FTR_12"], wk_day["COUNT_BUS"], marker ="+", c="grey", alpha=0.5, label='# Weekday Buses')
            ax.plot(wk_day["SEC_FTR_12"], yy, c="red", alpha=0.5, label='Line Best Fit: Weekday')


        # If Weekend Is Not Empty
        else:

            # Fit Curve To Data | Weekday
            curve = np.polyfit(wk_day["SEC_FTR_12"], wk_day["COUNT_BUS"], 15)
            poly = np.poly1d(curve)
            yy = poly(wk_day["SEC_FTR_12"])

            ax.scatter(wk_day["SEC_FTR_12"], wk_day["COUNT_BUS"], marker ="+", c="grey", alpha=0.5, label='# Weekday Buses')
            ax.plot(wk_day["SEC_FTR_12"], yy, c="red", alpha=0.5, label='Line Best Fit: Weekday')

            # Fit Curve To Data | Weekend
            curve = np.polyfit(wk_end["SEC_FTR_12"], wk_end["COUNT_BUS"], 15)
            poly = np.poly1d(curve)
            yy = poly(wk_end["SEC_FTR_12"])

            ax.scatter(wk_end["SEC_FTR_12"], wk_end["COUNT_BUS"], marker ="x", c="grey", alpha=0.5, label='# Weekend Buses')
            ax.plot(wk_end["SEC_FTR_12"], yy, c="blue", alpha=0.5, label='Line Best Fit: Weekend')

            ax.legend()

        # Manually Set X Ticks
        xlabels = [x for x in range(0, 26, 2)]
        xticks = [x*3600 for x in xlabels]
        xlabels = [f"{x}:00" for x in xlabels]
        ax.set_xticks(xticks, labels=xlabels)

        ax.set_xlabel("Time (24 Hour)")
        ax.set_ylabel("# Of Buses")

        fig.suptitle('Number Of Brampton Transit Buses Every 10 Minutes')
        ax.set_title(f"Data Collected Between: {dates_in[1]} & {dates_in[-1]}")

        # Save The Figure In Graphics Folder
        fig.savefig(f"{graphics_path}/NumBusesByHour.png")


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

        df.loc[df["MINUTE"] == "60", "MINUTE"] = "59"
        df.loc[df["MINUTE"] == "60", "SECOND"] = "59"

        # Create A New Datetime Timestamp | Keep Data That Was Recorded Yesterday | Delete Unneded Rows
        df["DT_COL"] = df['YEAR'] + "-" + df['MONTH'] + "-" + df['DAY'] + " " + df['HOUR'] + ":" + df['MINUTE'] + ":" + df['SECOND']
        df = df[df["DAY"] == str(ystrdy)].drop_duplicates().drop(['YEAR', 'MONTH', 'DAY'], axis=1)

        # Split Route Number From Columns
        df[["ROUTE", "ID"]] = df["route_id"].str.split('-', expand=True)
        df = df.drop(['ID'], axis=1)

        # Find The Number Of Buses Operating On A Given Route At A Every 10 Time Interval
        def num_ct(x): return len(x)
        grped_time = df.groupby(['ROUTE', 'HOUR', 'MINUTE', 'SECOND'], as_index=False).agg(
                                COUNT_BUS = ("ROUTE", num_ct)
                                )

        # Find The Number Of Routes Operating That Day
        num_routes = [x for x in np.unique(grped_time["ROUTE"])]
        gs = (grid_spec.GridSpec(num_routes,1))
        fig = plt.figure(figsize=(8,6))

        i = 0

        #creating empty list
        ax_objs = []

        # Iterate Through Each Route In Dataset
        for route in grped_time["ROUTE"].unique():

            # creating new axes object and appending to ax_objs
            ax_objs.append(fig.add_subplot(gs[i:i+1, 0:]))

            # plotting the distribution
            plot = (grped_time[grped_time["ROUTE"] == route]
                    .score.plot.kde(ax=ax_objs[-1], color="#f0f0f0", lw=0.5)
                   )

            # grabbing x and y data from the kde plot
            x = plot.get_children()[0]._x
            y = plot.get_children()[0]._y

            # filling the space beneath the distribution
            ax_objs[-1].fill_between(x,y)

            # setting uniform x and y lims
            ax_objs[-1].set_xlim(0, 1)
            ax_objs[-1].set_ylim(0,2.2)

            i += 1

        plt.tight_layout()
        plt.show()

        #
        # df = df[df["dt_colc"] == ystrdy]
        # print(df.head())
        # print(cur_dt_m2)
