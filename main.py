#!/usr/bin/env python

# Get Onboard Client provided by RTEMHackathon
import json
import os

from datetime import timedelta
from typing import Iterable, List, Union, Dict

from onboard.client import RtemClient

# Access API Key securely using config.ini file
import configparser
from pprint import pprint
import pandas as pd

# Load API Key using configparser
from onboard.client.dataframes import points_df_from_streaming_timeseries
from onboard.client.models import PointSelector, TimeseriesQuery, PointData
from requests_cache import CachedSession
from tqdm import tqdm
import requests_cache

config = configparser.ConfigParser()
config.read("config.ini")
api_key = config["DEFAULT"]["API_KEY"]

# Initialise Onboard Client
client = RtemClient(api_key=api_key)

session = CachedSession(
    "demo_cache",
    use_cache_dir=True,  # Save files in the default user cache dir
    cache_control=True,  # Use Cache-Control headers for expiration, if available
    expire_after=timedelta(days=1),  # Otherwise expire responses after one day
    allowable_methods=[
        "GET",
        "POST",
    ],  # Cache POST requests to avoid sending the same data twice
    allowable_codes=[
        200,
        400,
    ],  # Cache 400 responses as a solemn reminder of your failures
    ignored_parameters=[
        "api_key"
    ],  # Don't match this param or save it in the cache
    match_headers=True,  # Match all request headers
    stale_if_error=True,  # In case of request errors, use stale cache data if possible
)


def safe_open_w(path):
    """Open path for writing and create parent directories if required."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, "w")


def all_buildings_to_json():
    all_buildings = json.dumps(client.get_all_buildings())
    with safe_open_w("./data/json/all_buildings.json") as f:
        f.write(all_buildings)
    return


def get_all_building_ids():
    all_buildings = client.get_all_buildings()
    building_ids = [item["id"] for item in all_buildings]
    return building_ids


def get_all_buildings():
    building_ids = get_all_building_ids()
    for b_id in tqdm(building_ids):
        building_equipment = client.get_building_equipment(b_id)
        with safe_open_w(f"./api/buildings/{b_id}.json") as f:
            f.write(json.dumps(building_equipment))


def get_all_points_to_json():
    all_points = []
    building_ids = get_all_building_ids()
    for b_id in tqdm(building_ids):
        building_equipment = client.get_building_equipment(b_id)
        for b_equipment in building_equipment:
            b_points = b_equipment["points"]
            all_points.extend(b_points)
    with safe_open_w("./data/json/all_points.json") as f:
        f.write(json.dumps(all_points))


def get_all_points():
    all_points = []
    building_ids = get_all_building_ids()
    for b_id in tqdm(building_ids):
        building_equipment = client.get_building_equipment(b_id)
        for b_equipment in building_equipment:
            b_points = b_equipment["points"]
            all_points.extend(b_points)
    return all_points


def get_all_equipment():
    all_equipment = []
    building_ids = get_all_building_ids()
    for b_id in tqdm(building_ids):
        building_equipment = client.get_building_equipment(b_id)
        all_equipment.extend(building_equipment)
        with safe_open_w("./data/json/all_equipment.json") as f:
            f.write(json.dumps(all_equipment))


def get_timeseries_by_building(b_id):
    query = PointSelector()
    query.buildings = [b_id]
    selection = client.select_points(query)
    start = "2016-01-01T00:00:00Z"
    end = "2016-12-31T23:59:59Z"
    timeseries_query = TimeseriesQuery(
        point_ids=selection["points"], start=start, end=end
    )
    sensor_data = points_df_from_streaming_timeseries(
        client.stream_point_timeseries(timeseries_query)
    )
    sensor_data.to_json(f"./api/timeseries/2016_{b_id}.json")
    return


def get_timeseries_by_point(year, p_id):
    query = PointSelector()
    query.point_ids = [p_id]
    selection = client.select_points(query)
    start = f"{year}-01-01T00:00:00Z"
    end = f"{year}-12-31T23:59:59Z"
    timeseries_query = TimeseriesQuery(
        point_ids=selection["points"], start=start, end=end
    )
    sensor_data = points_data_from_streaming_timeseries(
        client.stream_point_timeseries(timeseries_query)
    )
    with safe_open_w(f"./api/timeseries/{p_id}/{year}.json") as f:
        f.write(json.dumps(sensor_data))
    return


def points_data_from_streaming_timeseries(
    timeseries: Iterable[PointData], points=[], point_column_label=None,
) -> pd.DataFrame:
    """Returns a pandas dataframe from the results of a timeseries query"""
    if point_column_label is None:

        def point_column_label(p):
            return p.get("id")

    point_names = {p["id"]: point_column_label(p) for p in points}
    columns: List[Union[str, int]] = ["timestamp"]
    dates = set()
    data_by_point = {}

    for point in timeseries:
        columns.append(point.point_id)
        ts_index = point.columns.index("time")
        data_index = point.columns.index(point.unit)

        point_data: Dict[str, Union[str, float, None]] = {}
        data_by_point[point.point_id] = point_data

        for val in point.values:
            ts: str = val[ts_index]  # type: ignore[assignment]
            dates.add(ts)
            clean = val[data_index]
            point_data[ts] = clean

    sorted_dates = list(dates)
    sorted_dates.sort()
    data = []

    for d in sorted_dates:
        row = {"timestamp": d}
        for p in columns[1:]:
            val = data_by_point[p].get(d)  # type: ignore
            point_col = point_names.get(p, p)
            row[point_col] = val  # type: ignore
        data.append(row)

    return data


def main():
    all_points = []
    with open('./api/points/all.json', 'r') as f:
        j_file = json.load(f)
        for point in j_file:
            all_points.append(point['id'])
        # print(j_file)
    for p_id in tqdm(all_points):
        print(f"Point Num: #{p_id}")
        for year in tqdm(range(2016, 2020)):
            print(f"Point: #{p_id}\tYear: {year}")
            get_timeseries_by_point(year, p_id)
    return


if __name__ == "__main__":
    main()
