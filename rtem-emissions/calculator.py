#!/usr/bin/env python
"""
calculator.py: Calculate NY Fuelmix for the year.
"""
import glob


# Get all fuelmix csv files for year.
import numpy
import pandas as pd


def get_fuelmix_csv(year):
    FILE_PATH = "../api/fuelmix"
    csv_list = []
    for file in glob.glob(f"{FILE_PATH}/{year}*rtfuelmix.csv"):
        print(file)
        csv_list.append(file)
    csv_list.sort()
    return csv_list


def main():
    dfs = []
    for csv_file in get_fuelmix_csv(2019):
        df = pd.read_csv(csv_file, names=['ts', 'tz', 'fuel_category', 'gen_MW'], index_col=None, header=0)
        df['gen_MWh'] = df['gen_MW'] * ( 5 / 60 )
        dfs.append(df)
    df = pd.concat(dfs, axis=0, ignore_index=True)
    return df


if __name__ == "__main__":
    df = main()
    print(df['gen_MWh'].sum())
    pt_df = pd.pivot_table(df, 'gen_MWh', 'fuel_category', aggfunc=numpy.sum)
    pt_df['gen_%'] = (pt_df['gen_MWh'] / pt_df['gen_MWh'].sum()) * 100
    pt_df.to_csv('../api/eia/2019_fuelmix_generation.csv')
    pass
