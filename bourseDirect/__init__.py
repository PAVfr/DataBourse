
from .market import Market
from .search import Search
from .sector import Sector


class OptionBourseDirect:
	LINK = "https://www.boursedirect.fr"


class BourseDirect(Market, Search):
	sector = Sector
