#!/usr/bin/env python
"""
client.py: Initialises the RTEM Onboard Client.
"""
import configparser
from onboard.client import RtemClient


class Client(RtemClient):
    def __init__(self, config_ini):
        self.config_path = config_ini
        self._parse_config()
        super(Client, self).__init__(self.api_key)

    def _parse_config(self):
        config = configparser.ConfigParser()
        config.read(self.config_path)
        self.api_key = config['DEFAULT']['API_KEY']
