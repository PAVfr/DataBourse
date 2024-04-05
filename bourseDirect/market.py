
from options import Options
from .search import Search


class Market:
	@classmethod
	def CAC_ALL_TRADABLE(cls):
		data = Options.requestJson(
			"https://www.boursedirect.fr/api/instrument/list/europe.france.components.cac-all-tradable-FR0003999499-CACT-EUR-XPAR")
		return [ResultMarket(v) for v in data.get("instruments")]

	@classmethod
	def CAC_40(cls):
		"""Le CAC 40 reflète la performance des 40 actions les plus importantes."""
		data = Options.requestJson(
			"https://www.boursedirect.fr/api/instrument/list/europe.france.components.cac-40-FR0003500008-PX1-EUR-XPAR")
		return [ResultMarket(v) for v in data.get("instruments")]

	@classmethod
	def SBF_120(cls):
		"""Le SBF 120 est un indice boursier, composé de toutes les sociétés du CAC Large 60 ET du CAC Mid 60."""
		data = Options.requestJson(
			"https://www.boursedirect.fr/api/instrument/list/europe.france.components.sbf-120-FR0003999481-PX4-EUR-XPAR")
		return [ResultMarket(v) for v in data.get("instruments")]

	@classmethod
	def CAC_LARGE_60(cls):
		"""Le CAC Large 60 est un indice boursier qui regroupe les 60 entreprises du CAC 40 et du CAC Next 20."""
		data = Options.requestJson(
			"https://www.boursedirect.fr/api/instrument/list/europe.france.components.cac-large-60-QS0011213657-CACLG-EUR-XPAR")
		return [ResultMarket(v) for v in data.get("instruments")]

	@classmethod
	def CAC_MID_60(cls):
		"""
		Le CAC Mid 60 regroupe soixante entreprises d'importance nationale ou européenne.
		Il suit immédiatement le CAC 40 et le CAC Next 20 et forme avec eux le SBF 120.
		"""
		data = Options.requestJson(
			"https://www.boursedirect.fr/api/instrument/list/europe.france.components.cac-mid-60-QS0010989117-CACMD-EUR-XPAR")
		return [ResultMarket(v) for v in data.get("instruments")]

	@classmethod
	def CAC_MID_and_SMALL(cls):
		data = Options.requestJson(
			"https://www.boursedirect.fr/api/instrument/list/europe.france.components.cac-mid-small-QS0010989133-CACMS-EUR-XPAR")
		return [ResultMarket(v) for v in data.get("instruments")]

	@classmethod
	def CAC_SMALL(cls):
		data = Options.requestJson(
			"https://www.boursedirect.fr/api/instrument/list/europe.france.components.cac-small-QS0010989125-CACS-EUR-XPAR")
		return [ResultMarket(v) for v in data.get("instruments")]

	@classmethod
	def CAC_NEXT_20(cls):
		"""
		Le CAC Next 20 regroupe les vingt valeurs dont l'importance arrive après le CAC 40.
		"""
		data = Options.requestJson(
			"https://www.boursedirect.fr/api/instrument/list/europe.france.components.cac-next-20-QS0010989109-CN20-EUR-XPAR")
		return [ResultMarket(v) for v in data.get("instruments")]

	@classmethod
	def PEA_PME(cls):
		"""Retourne la liste des entreprises éligibles au PEA-PME en France"""
		return [ResultMarket(v) for v in Search.searchV3(params={"peapme": "true"})]


class ResultMarket:
	def __init__(self, data: dict):
		from . import OptionBourseDirect
		self.isin: str = data.get("isin")
		self.exchange: dict = data.get("market")
		self.name: str = data.get("name")
		self.slug: str = data.get("slug")
		self.ticker: str = data.get("ticker")
		self.url: str = OptionBourseDirect.LINK + data.get("url")
