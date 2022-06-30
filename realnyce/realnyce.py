import inflection
import pandas as pd

from realnyce.client import Client
from datetime import datetime
from tqdm import tqdm

from realnyce.emissions import EmissionIntensity

BASE_PATH = "../api"

client = Client(config_ini="../config.ini")


class BuildingData:
    def __init__(self, rtem_client, building_id):
        self.info = {}
        self.client = rtem_client
        self.building_id = building_id
        self._get_info()
        self._get_equipment()
        self._get_points()
        # self._get_update_times()
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
            for point_id in equipment["points"]:
                self.points.append(point_id)

    def _get_update_times(self):
        update_times = []
        for point_id in self.points:
            update_times.append(point_id["first_updated"])
            update_times.append(point_id["last_updated"])
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


class TimeseriesData:
    def __init__(self, points=None, start_time=None, end_time=None):
        self.date_range = None
        self.start_time = start_time
        self.end_time = end_time
        self.points = set()
        self._append_points(points)
        self.df = None

    def _append_points(self, points):
        if points is not None:
            for point in points:
                self.points.add(point)

    def get_point_data(self, point_id, use_local=True):
        self.points.add(point_id)
        if use_local is True:
            DATA_PATH = "../api/timeseries/all"
            try:
                df = pd.read_csv(
                    f"{DATA_PATH}/{point_id}.csv",
                    usecols=["timestamp", str(point_id)],
                    parse_dates=["timestamp"],
                    index_col="timestamp",
                )
                if self.df is None:
                    self.df = df
                else:
                    self.df = self.df.join(df, how="outer")
            finally:
                print("Got point data.")

        else:
            raise Exception("Uh oh. We haven't written that feature yet.")

    def load(self, points=None):
        self._append_points(points)
        if len(self.points) == 0:
            print("The Points List is Empty.")
        else:
            for point_id in self.points:
                self.get_point_data(point_id)

    def _update_date_range(self):
        if self.df is not None:
            s_t = self.df.index.min()
            self.start_time = s_t.tz_convert("America/New_York")
            e_t = self.df.index.max()
            self.end_time = e_t.tz_convert("America/New_York")
            self.date_range = pd.date_range(self.start_time, self.end_time)
            self.date_range = [d.strftime("%Y%m%d") for d in self.date_range]
        else:
            print("self.df is empty.")

    def get_loadmix_data(self):
        FOSSIL_FUELS = [
            ("Gen_MWh", "Dual Fuel"),
            ("Gen_MWh", "Natural Gas"),
            ("Gen_MWh", "Other Fossil Fuels"),
        ]
        loadmix_df = self._extract_loadmix_data()
        ei = EmissionIntensity()
        df = loadmix_df.pivot_table(
            index=loadmix_df.index, columns="fuel_category", values=["gen_MW"]
        )
        list_cols = list(df)
        for col in list_cols:
            gen_MW, fuel_category = col
            df[("Pct", fuel_category)] = df[col] / df[list_cols].sum(axis=1)
            df[("Gen_MWh", fuel_category)] = df[col] * (5 / 60)
            print(fuel_category)
        df[("Grams CO2", "Total")] = (
            (df[("Gen_MWh", "Dual Fuel")] * ei.dual_fuel["co2_g_kWh"] * 1_000)
            + (
                df[("Gen_MWh", "Natural Gas")]
                * ei.natural_gas["co2_g_kWh"]
                * 1_000
            )
            + (
                df[("Gen_MWh", "Other Fossil " "Fuels")]
                * ei.other_fossil["co2_g_kWh"]
                * 1_000
            )
        )
        df[("Grams GHG", "Total")] = (
            (df[("Gen_MWh", "Dual Fuel")] * ei.dual_fuel["ghg_g_kWh"] * 1_000)
            + (
                df[("Gen_MWh", "Natural Gas")]
                * ei.natural_gas["ghg_g_kWh"]
                * 1_000
            )
            + (
                df[("Gen_MWh", "Other Fossil " "Fuels")]
                * ei.other_fossil["ghg_g_kWh"]
                * 1_000
            )
        )
        df[("Gen_MWh", "Total")] = df[list_cols].sum(axis=1) * (5 / 60)
        df[("Grams CO2 per kWh", "Average")] = (
            df[("Grams CO2", "Total")] / df[("Gen_MWh", "Total")]
        ) / 1_000
        df[("Grams GHG per kWh", "Average")] = (
            df[("Grams GHG", "Total")] / df[("Gen_MWh", "Total")]
        ) / 1_000
        df.to_csv("../api/report/historic_loadmix.csv")
        df = pd.concat(
            [
                df[("Grams CO2 per kWh", "Average")],
                df[("Grams GHG per kWh", "Average")],
            ],
            axis=1,
        )
        df.to_csv("../api/report/historic_emissions.csv")
        return df

    def _extract_loadmix_data(self):
        BASE_PATH = "../api/fuelmix"

        self._update_date_range()
        loadmix_df = None
        for dt in tqdm(self.date_range):
            df = pd.read_csv(
                f"{BASE_PATH}/{dt}rtfuelmix.csv",
                names=["ts", "tz", "fuel_category", "gen_MW"],
                header=0,
            )
            df["tz"].replace("EST", "-4:00", inplace=True)
            df["tz"].replace("EDT", "-5:00", inplace=True)
            df["ts"] = df["ts"] + df["tz"]
            df.drop(labels=["tz"], axis=1, inplace=True)
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
            # df['ts'].dt.tz_convert('UTC')
            df = df.set_index("ts")
            if loadmix_df is None:
                loadmix_df = df
                print("")
            else:
                loadmix_df = pd.concat([loadmix_df, df])
        return loadmix_df


class RealNYCE(BuildingData):
    def __init__(self, rtem_client, building_id):
        super().__init__(rtem_client, building_id)
        self.elec_meters = None
        self._get_elec_consump_meters()

    def _get_elec_consump_meters(self):
        elec_consump_meters = []
        for equip in self.equipment:
            if equip["equip_subtype_tag"] == "elecMeter":
                for point in equip["points"]:
                    if point["type"] == "Electric Consumption":
                        elec_consump_meters.append(point)

        self.elec_meters = elec_consump_meters
        return elec_consump_meters


def main():
    building = BuildingData(client, 426)
    power_meters = []
    for point in building.points:
        if point["type"] in [
            "Outside Air Temperature",
        ]:
            power_meters.append(point["id"])
    dfs = []
    for meter in power_meters:
        try:
            FILE_PATH = f"{BASE_PATH}/timeseries/all/{meter}.csv"
            df = pd.read_csv(
                FILE_PATH,
                infer_datetime_format=True,
                usecols=[1, 2],
                parse_dates=["timestamp"],
            )
            df = df.set_index("timestamp")
            df.index = pd.to_datetime(df.index)
            dfs.append(df)
        finally:
            pass
    df = pd.concat(dfs, axis=1)
    whoami = client.whoami()


def functionality():
    # Initialise RealNYCE with RTEM Client
    # Params:
    # client:   RTEM Client
    # 426:      Target Building Id
    nyce = RealNYCE(client, 426)
    # Empty list to store Energy Meters
    energy_demand_meters = []
    # Loop over Elec. Meters list
    for meter in nyce.elec_meters:
        # Add "Demand" meters to list
        if "Demand" in meter["name"]:
            energy_demand_meters.append(meter)

    # TimeseriesData class gathers and processes Building data
    td = TimeseriesData(points=energy_demand_meters)
    td.load()

    # loadmix_df contains a DataFrame with emission intensity data
    loadmix_df = td.get_loadmix_data()
    # Join with Timeseries data
    joined_df = pd.concat([td.df, loadmix_df], axis=1)
    # Drop empty cells
    joined_df.dropna(inplace=True)
    # Return final DataFrame
    return joined_df


if __name__ == "__main__":
    functionality()
