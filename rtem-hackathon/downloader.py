import time
from datetime import datetime
import configparser
from os.path import exists
import asyncio
import pandas as pd
import inflection
import functions
from tqdm import tqdm
from onboard.client import RtemClient
from onboard.client.models import TimeseriesQuery, PointData, PointSelector
from onboard.client.dataframes import points_df_from_streaming_timeseries


# Load API Key using configparser
config = configparser.ConfigParser()
config.read("../config.ini")
api_key = config["DEFAULT"]["API_KEY"]
# Initialise Onboard Client
client = RtemClient(api_key=api_key)


class BuildingData:
    def __init__(self, rtem_client, building_id):
        self.info = {}
        self.client = rtem_client
        self.building_id = building_id
        self._get_info()
        self._get_equipment()
        self._get_points()
        self._get_update_times()
        self._get_equipment_types()
        self._get_point_types()
        self.dl_count = 0
        self.dl_limit = 100

    def _get_info(self):
        all_buildings: list = self.client.get_all_buildings()
        for building_info in all_buildings:
            if building_info["id"] == self.building_id:
                for key, value in dict(building_info).items():
                    if value is None:
                        del building_info[key]

                for key, value in dict(building_info["info"]).items():
                    if len(value) == 0:
                        del building_info["info"][key]
                    else:
                        new_key = inflection.underscore(key)
                        building_info[new_key] = value

                del building_info["info"]
                self.info = building_info

    def _get_equipment(self):
        self.equipment = client.get_building_equipment(self.building_id)
        pass

    def _get_points(self):
        self.points = []
        for equipment in self.equipment:
            for point in equipment["points"]:
                self.points.append(point)

    def _get_update_times(self):
        update_times = []
        for point in self.points:
            update_times.append(point["first_updated"])
            update_times.append(point["last_updated"])
        self.info["first_update"] = min(update_times)
        self.info["last_update"] = max(update_times)
        self.first_updated = datetime.fromtimestamp(
            self.info["first_update"] / 1000
        )
        self.last_updated = datetime.fromtimestamp(
            self.info["last_update"] / 1000
        )

    def _get_equipment_types(self):
        equipment_types = set()
        for item in self.equipment:
            equipment_types.add(item["equip_type_name"])
        self.equipment_types = equipment_types

    def _get_point_types(self):
        point_types = set()
        for point in self.points:
            point_types.add(point["type"])
        self.point_types = point_types

    def get_all_point_data(self):
        for point in self.points:
            if exists(f"../api/timeseries/all/{point['id']}.csv"):
                print(
                    f"{datetime.now()}\t[{point['id']}]: Exists already. Skipping..."
                )
            else:
                if self.dl_count <= self.dl_limit:
                    functions.get_all_point_data(point["id"])
                    self.dl_count += 1
                    print(
                        f"{datetime.now()}\t[{point['id']}] Current Count: {self.dl_count}."
                    )
                else:
                    try:
                        print(
                            f"{datetime.now()}\t[{point['id']}] Download Limit Reached! Pausing..."
                        )
                        minutes_slept = 0
                        sleep_duration = 60
                        while minutes_slept <= 60:
                            time.sleep(300)
                            minutes_slept += 5
                            print(
                                f"{datetime.now()}\tSlept for {minutes_slept}. {sleep_duration - minutes_slept} to "
                                f"go."
                            )
                        self.dl_count = 0

                    except KeyboardInterrupt:
                        quit()


class DownloadRate:
    def __init__(self):
        self.max_rate = 120
        self.dl_count = 0
        self.time_elapsed = 0
        self.dl_rate = None
        self.start_time = datetime.now()

    def _update_time_elapsed(self):
        diff = datetime.now() - self.start_time
        self.time_elapsed = diff.total_seconds()

    def current_download_rate(self):
        self.dl_rate = self.dl_count / (self.time_elapsed * 3600)
        return self.dl_rate

    def increment_dl_count(self):
        self.dl_count += 1
        self._update_time_elapsed()


# bd_440 = BuildingData(client, 440)
# bd_440.get_all_point_data()
# bd_426 = BuildingData(client, 426)
# bd_440.get_all_point_data()

sample_buildings = [426, 440, 112, 217, 100, 154, 373, 442, 134, 316]
interesting_buildings = [
    441,
    110,
    127,
    120,
    477,
    141,
    230,
    423,
    426,
    438,
    440,
    503,
    141,
    118,
    128,
    134,
    445,
    484,
    434,
    271,
    114,
    419,
    327,
]

if __name__ == "__main__":
    for building_id in sample_buildings:
        BuildingData(client, building_id).get_all_point_data()
    for building_id in interesting_buildings:
        BuildingData(client, building_id).get_all_point_data()

# for building_id in interesting_buildings:
#     # Initialise BuildingData Object
#     building = BuildingData(client, building_id)
#     # Retrieve start and end dates for Timeseries Query
#     dt_start = building.first_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
#     dt_end = building.last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
#     # Retrieve Point IDs for Building
#     point_ids = [point["id"] for point in building.points]
#     # Build Timeseries Query
#     ts_query = TimeseriesQuery(point_ids=point_ids, start=dt_start, end=dt_end)
#     # Timeseries Query Response
#     ts_response = client.stream_point_timeseries(ts_query)
#     # Stream Sensor Data from Timeseries Query to DataFrame
#     ts_sensor_data = points_df_from_streaming_timeseries(ts_response)
#     # Write Sensor Data to CSV File
#     ts_sensor_data.to_csv(f"../api/buildings/{building_id}.csv")
#     print(
#         f"{datetime.now()}\t[{building_id}] Done: Written to '../api/buildings/{building_id}.csv'"
#     )

pass
