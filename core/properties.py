import configparser

class configuration:
    host = None
    database = None
    user = None
    port = None
    password = None
    apache_folder = None

    def __init__(self) -> None:
        config = configparser.ConfigParser()
        config.read('app.ini')
        ambiente = str(config['ambiente']['ambiente'])
        self.host = str(config[ambiente]['host'])
        self.database = str(config[ambiente]['database'])
        self.user = str(config[ambiente]['user'])
        self.port = str(config[ambiente]['port'])
        self.password = str(config[ambiente]['password'])
        self.apache_folder = str(config[ambiente]['apache_folder'])