import configparser
from collections import OrderedDict as _default_dict

class ReadConfig(object):

	DEFAULTSECT = "config_file"
	
	def __init__(self, dict_type=_default_dict):
		self.configparser = None
		self._dict = dict_type
		self._sections = self._dict()
		self._defaults = self._dict()

	def readConfiguration(self, config_file):
		self.confparser = configparser.ConfigParser()
		self.confparser.read(config_file)

	def getValue(self, option):
		return self.confparser.get(ReadConfig.DEFAULTSECT, option)

	def sections(self):
		return self._sections.keys()

	def options(self, section):
		opts = self._sections[ReadConfig.DEFAULTSECT].copy()

		opts.update(self._defaults)
		if '__name__' in opts:
			del opts['__name__']
		return opts.keys()

# def getOptions(config):
# 	opt = []
# 	for section in config.sections():
# 		for options in config.options(section):
# 			opt.append(config.get(section, options))
# 	return opt

# if __name__ == "__main__":
#     # config = configparser.ConfigParser()
#     # config.read("config.ini")
#     # opt = getOptions(config)
#     # print(opt)

#     conf = ReadConfig()
#     conf.readConfiguration("config.ini")
#     print(conf.getValue("port"))
