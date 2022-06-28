#!/usr/bin/env python
"""
estimator.py: Calculate GHG emissions intensity by energy source.
"""
import pandas as pd


class AnnualEmissions:
    FILE_PATH = "../api/eia/emission_annual.xls"

    def __init__(self):
        pass

    def to_df(self):
        ae_df = pd.read_excel(
            self.FILE_PATH,
            names=[
                "year",
                "state",
                "producer_type",
                "energy_source",
                "co2",
                "so2",
                "nox",
            ],
        )
        return ae_df


class AnnualGeneration:
    FILE_PATH = "../api/eia/annual_generation_state.xls"

    def __init__(self):
        pass

    def to_df(self):
        ag_df = pd.read_excel(
            self.FILE_PATH,
            skiprows=1,
            names=[
                "year",
                "state",
                "producer_type",
                "energy_source",
                "generation_mWh",
            ],
        )
        return ag_df


def estimator():
    ae_df = AnnualEmissions().to_df()
    ag_df = AnnualGeneration().to_df()

    ny_ae_df = ae_df[
        (ae_df["state"] == "NY")
        & (ae_df["producer_type"] == "Total Electric Power Industry")
    ]
    ny_ag_df = ag_df[
        (ag_df["state"] == "NY")
        & (ag_df["producer_type"] == "Total Electric Power Industry")
    ]

    ny_ag_df.loc[
        ny_ag_df.energy_source == "Total", "energy_source"
    ] = "All Sources"

    ny_ae_df = ny_ae_df.set_index(
        ["year", "state", "producer_type", "energy_source"]
    )
    ny_ag_df = ny_ag_df.set_index(
        ["year", "state", "producer_type", "energy_source"]
    )
    df = ny_ag_df.join(ny_ae_df, how="outer")

    # Calculate column 'co2_g_kWh' Grams CO2 per kWh
    df["co2_g_kWh"] = (df["co2"] * 1_000_000) / (df["generation_mWh"] * 1_000)
    df["ghg_g_kWh"] = (
        (df["co2"] + df["so2"] + df["nox"])
        * 1_000_000
        / (df["generation_mWh"] * 1_000)
    )
    df = df.fillna(0).reset_index()
    df_2019 = df.loc[df["year"] == 2019]
    df_2019.to_csv('../api/eia/2019_emissions_generation.csv')
    return df_2019


if __name__ == "__main__":
    df = estimator()
    print(df)
