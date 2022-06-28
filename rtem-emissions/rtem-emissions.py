from client import Client

if __name__ == '__main__':
    client = Client(config_ini='../config.ini')
    whoami = client.whoami()
