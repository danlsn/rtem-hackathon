from datetime import datetime
from time import perf_counter
from onboard.client import RtemClient
import configparser
import asyncio
import pandas as pd
from onboard.client.models import TimeseriesQuery, PointData, PointSelector
from onboard.client.dataframes import points_df_from_streaming_timeseries


# Load API Key using configparser
config = configparser.ConfigParser()
config.read("../config.ini")
api_key = config["DEFAULT"]["API_KEY"]
# Initialise Onboard Client
client = RtemClient(api_key=api_key)


def building_points(building_id: int) -> list:
    query = PointSelector()
    query.buildings = [building_id]
    selection = client.select_points(query)
    return selection["points"]


def get_point_info(point_id: int):
    res = client.get_points_by_ids([point_id])[0]
    dt_start = datetime.fromtimestamp(
        res["first_updated"] / 1000 - 3600
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    dt_end = datetime.fromtimestamp(
        res["last_updated"] / 1000 + 3600
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    point_info = {
        "id": res["id"],
        "b_id": res["building_id"],
        "dt_start": dt_start,
        "dt_end": dt_end,
    }
    return point_info


def timeseries_query(point_id):
    point_info: dict = get_point_info(point_id)
    p_id, b_id, dt_start, dt_end = point_info.values()
    ts_query = TimeseriesQuery(point_ids=[p_id], start=dt_start, end=dt_end)
    return ts_query


def get_all_point_data(point_id: int):
    p_id = point_id
    ts_query = timeseries_query(p_id)
    ts_res = client.stream_point_timeseries(ts_query)
    sensor_data = points_df_from_streaming_timeseries(ts_res)
    sensor_data.to_csv(f"../api/timeseries/all/{p_id}.csv")
    print(
        f"{datetime.now()}\t[{p_id}] Done: Written to '../api/timeseries/all/{p_id}.csv'"
    )
    return


async def worker(name, queue):
    while True:
        work_item = await queue.get()
        print(f"{datetime.now()}\t{name} is working on: {work_item}")
        await get_all_point_data(work_item)
        queue.task_done()
        print(f"{datetime.now()}\tTask #{work_item} is done.")


async def points_queue(points_list):
    try:
        queue = asyncio.Queue()
        for p_id in points_list:
            queue.put_nowait(p_id)
        tasks = []
        for i in range(6):
            task = asyncio.create_task(worker(f"worker-{i}", queue))
            tasks.append(task)

        # Wait until the queue is fully processed.
        started_at = perf_counter()
        print(f"Started at: {datetime.now()}\n============")
        await queue.join()
        total_time = perf_counter() - started_at
    except KeyboardInterrupt:
        print("Exiting...")
        exit

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

    print(f"Total Time: {total_time}")


if __name__ == "__main__":
    points_list = building_points(441)
    asyncio.run(points_queue(points_list))
