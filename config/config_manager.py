import configparser
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

DEFAULT_CONFIG_PATH = Path(__file__).parent / "settings.ini"

class ConfigTools:
    def __init__(self, config_file: str | Path = DEFAULT_CONFIG_PATH ) -> None:
        self._config = configparser.ConfigParser()
        self._config_file = Path(config_file)
        self._load_confg()
    
    def _load_confg(self) -> None:
        """加载配置文件，如果文件不存在则创建"""
        try:
            if self._config_file.exists():
                self._config.read(self._config_file, encoding='utf-8')
            else:
                self._config_file.touch()
        except Exception as e:
            raise ConfigError(f"无法加载配置文件: {e}")
    
    def get_config(self, section: str, key: str, default: Any = None) -> Any:
        """
        获取配置值
        :param section: 配置节
        :param key: 配置键
        :param default: 默认值
        :return: 配置值
        """
        try:
            return self._config[section][key]
        except KeyError:
            return default
    
    def set_config(self, section: str, key: str, value: Any) -> None:
        """
        设置配置值
        :param section: 配置节
        :param key: 配置键
        :param value: 配置值
        """
        try:
            if section not in self._config:
                self._config.add_section(section)
            
            self._config[section][key] = str(value)
            self._save_config()
        except Exception as e:
            raise ConfigError(f"设置配置失败: {e}")
    
    def _save_config(self) -> None:
        """保存配置到文件"""
        try:
            with open(self._config_file, "w", encoding='utf-8') as f:
                self._config.write(f)
        except Exception as e:
            raise ConfigError(f"保存配置文件失败: {e}")
    
    def remove_option(self, section: str, key: str) -> bool:
        """
        删除指定的配置项
        :param section: 配置节
        :param key: 配置键
        :return: 是否删除成功
        """
        try:
            return self._config.remove_option(section, key)
        except Exception:
            return False
    
    def get_sections(self) -> list[str]:
        """获取所有配置节"""
        return self._config.sections()


class ConfigError(Exception):
    """配置相关的异常"""
    pass


if __name__ == "__main__":
    config = ConfigTools()
    # 测试配置操作
    config.set_config("Running.Settings", "LastTradeDate", datetime.now().strftime("%Y%m%d"))
    print(config.get_config("Running.Settings", "LastTradeDate"))
    print(config.get_sections())



