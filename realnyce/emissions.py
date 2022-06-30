#!/usr/bin/env python
"""
emission.py: Calculate ghg emission intensity for given timeperiod energy usage data.
"""
from datetime import datetime, timedelta
import time
from realnyce.fetcher import fetch_current_fuelmix
import pytz
import pandas as pd

FUEL_CAT_MAP = {
    "Dual Fuel": "dual_fuel",
    "Hydro": "hydro",
    "Natural Gas": "natural_gas",
    "Nuclear": "nuclear",
    "Other Fossil Fuels": "other_fossil",
    "Other Renewables": "other_renewables",
    "Wind": "wind"
}


def get_row(df, fuel_category):
    return df.loc[df["fuel_category"] == fuel_category].to_dict("records")[0]


def round_down_15(timestamp: datetime):
    ts = timestamp - timedelta(
        minutes=(timestamp.minute % 5) - 15,
        seconds=timestamp.second,
        microseconds=timestamp.microsecond,
    )
    return ts


class EmissionIntensity:
    CURRENT_EMISSIONS = "../api/emissions/current.csv"

    def __init__(self):
        self.current_ei = None
        df = pd.read_csv(self.CURRENT_EMISSIONS)
        self.dual_fuel = get_row(df, "Dual Fuel")
        self.hydro = get_row(df, "Hydro")
        self.natural_gas = get_row(df, "Natural Gas")
        self.nuclear = get_row(df, "Nuclear")
        self.other_fossil = get_row(df, "Other Fossil Fuels")
        self.other_renewables = get_row(df, "Other Renewables")
        self.wind = get_row(df, "Wind")

    def load_df(self, df):
        df["gen_MWh"] = df["gen_MW"] * (5 / 60)
        df['gen_pct'] = (df['gen_MW'] / df['gen_MW'].sum()) * 100
        df['co2_g_kWh'] = ''
        df['ghg_g_kWh'] = ''

        print(self.__dict__)
        for val in df['fuel_category']:
            fuel_category = FUEL_CAT_MAP[val]
            ei_dict = self.__dict__
            co2_g_kWh = ei_dict[fuel_category]['co2_g_kWh']
            ghg_g_kWh = ei_dict[fuel_category]['ghg_g_kWh']
            df['co2_g_kWh'].loc[df['fuel_category'] == val] = co2_g_kWh
            df['ghg_g_kWh'].loc[df['fuel_category'] == val] = ghg_g_kWh
        self.current_ei = df


class FuelMix:
    def __init__(self):
        pass

    def current(self):
        pass

    def at(self, timestamp):
        pass


def main():
    df_fuelmix = fetch_current_fuelmix()
    ei = EmissionIntensity()
    ei.load_df(df_fuelmix)


if __name__ == "__main__":
    main()
