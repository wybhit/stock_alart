import configparser
from datetime import datetime

class ConfigTools:
    config = configparser.ConfigParser()
    config.read("settings.ini")
    

    @classmethod
    def get_config(self,section,key):
        return self.config[section][key]
   
    @classmethod
    def set_config(self,section,key,value):
        self.config[section][key] = value
        with open("settings.ini", "w") as f:
            self.config.write(f)    


if __name__ == "__main__":
   
    ConfigTools.set_config("Running.Settings","LastTradeDate","20241122")
    print(ConfigTools.get_config("Running.Settings","LastTradeDate"))

