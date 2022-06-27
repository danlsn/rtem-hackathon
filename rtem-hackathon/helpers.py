from onboard.client import RtemClient
import configparser


def authenticate_client():
    config = configparser.ConfigParser()
    config.read("../config.ini")
    api_key = config["DEFAULT"]["API_KEY"]
    return RtemClient(api_key=api_key)
