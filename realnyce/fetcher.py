#!/usr/bin/env python
"""
fetcher.py: Fetch data from EIA, NYISO, and RTEM.
"""

# Base directory path for file downloads.
from os.path import exists
import tempfile
import zipfile
import pandas as pd
import requests
from datetime import datetime

BASE_PATH = "../api"
# EIA Annual Generation & Emissions URL
ANNUAL_EMISSIONS_URL = (
    "https://www.eia.gov/electricity/data/state/emission_annual.xls"
)
ANNUAL_GENERATION_URL = (
    "https://www.eia.gov/electricity/data/state/annual_generation_state.xls"
)

# NYISO Download URLs
NY_FUELMIX_URL = "http://mis.nyiso.com/public/csv/rtfuelmix"  # 'http://mis.nyiso.com/public/csv/rtfuelmix/20220601rtfuelmix_csv.zip'
NY_RT_LBMP_URL = "http://mis.nyiso.com/public/csv/rtlbmp"  # 'http://mis.nyiso.com/public/csv/rtlbmp/20220601rtlbmp_zone_csv.zip'
NY_LBMP_URL = "http://mis.nyiso.com/public/csv/realtime"


def download_file(url, dir):
    local_filename = f"{dir}/{url.split('/')[-1]}"
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                # if chunk:
                f.write(chunk)
    return local_filename


def extract_zipfile(zip_path, destination):
    zip_file = zipfile.ZipFile(zip_path)
    zip_file.extractall(destination)


# Unzip NYISO Archive Downloads


class Data:
    DL_PATH = "../api"
    BASE_URL = ""

    def __init__(self):
        self._set_download_path(path=self.DL_PATH)

    def _set_download_path(self, path="../api"):
        self.DL_PATH = path

    def download(self):
        download_file(self.BASE_URL, self.DL_PATH)


class ArchiveData(Data):
    DL_PATH = "../api"
    BASE_URL = ""
    FILE_SUFFIX = ""
    ARCHIVE_SUFFIX = ""

    def __init__(self, start_date, end_date):
        self._set_download_path(path=self.DL_PATH)
        self.start_date = start_date
        self.end_date = end_date
        self.ms_list = self._month_start_list()

    def _download_all_archives(self):
        # Download All Fuelmix Archives for Given Period
        with tempfile.TemporaryDirectory() as tmpdir:
            print(f"Temp. Directory: {tmpdir}")
            for ms in self.ms_list:
                month_start = ms.strftime("%Y%m%d")
                file_name = month_start + self.FILE_SUFFIX
                archive_name = month_start + self.ARCHIVE_SUFFIX
                print(f"Downloading Archive for {month_start}.")
                if exists(f"{self.DL_PATH}/{file_name}"):
                    print(f"File Exists: {file_name}")
                    continue
                # Download Zip files to a Temporary Directory
                try:
                    download_file(
                        f"{self.BASE_URL}/{archive_name}", tmpdir,
                    )
                    print(f"Download Successful for {archive_name}")
                    self._extract(f"{tmpdir}/{archive_name}")
                    print(f"Extracted to {self.DL_PATH}")
                except requests.exceptions.HTTPError as err:
                    print(err)

    def _extract(self, zip_path):
        destination = self.DL_PATH
        zip_file = zipfile.ZipFile(zip_path)
        zip_file.extractall(destination)

    def _month_start_list(self):
        dates = pd.date_range(self.start_date, self.end_date, freq="MS")
        return dates

    def download(self):
        self._download_all_archives()


class FuelMixData(ArchiveData):
    BASE_URL = NY_FUELMIX_URL
    DL_PATH = "../api/fuelmix"
    FILE_SUFFIX = "rtfuelmix.csv"
    ARCHIVE_SUFFIX = "rtfuelmix_csv.zip"

    def __init__(self, start_date, end_date):
        super().__init__(start_date, end_date)


class EmissionsData(Data):
    BASE_URL = ANNUAL_EMISSIONS_URL
    DL_PATH = "../api/eia"

    def __init__(self):
        pass


class GenerationData(Data):
    BASE_URL = ANNUAL_GENERATION_URL
    DL_PATH = "../api/eia"

    def __init__(self):
        pass


class RTLBMPData(ArchiveData):
    BASE_URL = NY_RT_LBMP_URL
    DL_PATH = "../api/rtlbmp"
    FILE_SUFFIX = "rtlbmp_zone.csv"
    ARCHIVE_SUFFIX = "rtlbmp_zone_csv.zip"

    def __init__(self, start_date, end_date):
        super().__init__(start_date, end_date)


class LBMPData(ArchiveData):
    BASE_URL = NY_LBMP_URL
    DL_PATH = "../api/lbmp"
    FILE_SUFFIX = "realtime_zone.csv"
    ARCHIVE_SUFFIX = "realtime_zone_csv.zip"

    def __init__(self, start_date, end_date):
        super().__init__(start_date, end_date)


def fetch_current_fuelmix():
    now = datetime.now().strftime("%Y%m%d")
    filename = f"{now}rtfuelmix.csv"
    download_url = f"{NY_FUELMIX_URL}/{filename}"
    with tempfile.TemporaryDirectory() as tmpdir:
        download_file(download_url, tmpdir)
        df = pd.read_csv(
            f"{tmpdir}/{filename}",
            names=["ts", "tz", "fuel_category", "gen_MW"],
            index_col=None,
            header=0,
        )
        ts_max = df["ts"].max()
        df = df.loc[df["ts"] == ts_max]
        return df


def fetch_building_data():
    pass


def main():
    # df = fetch_current_fuelmix()
    # pass
    # EmissionsData().download()
    # GenerationData().download()
    lbmp = LBMPData('1999-11-01', '2022-06-01')
    lbmp.download()

if __name__ == "__main__":
    main()
