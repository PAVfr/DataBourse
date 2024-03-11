
from options import Options
from .search import Search


class Sector:
	@classmethod
	def Biens_de_consommation(cls):
		params = {"industry": "Biens de consommation"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Finances(cls):
		params = {"industry": "Finances"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Industriels(cls):
		params = {"industry": "Industriels"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Matieres_de_base(cls):
		params = {"industry": "Matières de base"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Petrole_et_gaz(cls):
		params = {"industry": "Pétrole et gaz"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Services_aux_consommateurs(cls):
		params = {"industry": "Services aux consommateurs"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Soins_de_sante(cls):
		params = {"industry": "Soins de santé"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Technologie(cls):
		params = {"industry": "Technologie"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Telecommunications(cls):
		params = {"industry": "Télécommunications"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def Utilitaires(cls):
		params = {"industry": "Utilitaires"}
		return [ResultSector(v) for v in [{**params, **line} for line in Search.searchV3(params=params)]]

	@classmethod
	def all_sector_to_dict(cls):
		"""Retourne un dictionnaire, avec tous les ticket rangé dans leur secteur"""
		data = {}
		for results in [cls.Biens_de_consommation(), cls.Finances(), cls.Industriels(), cls.Matieres_de_base() +
			cls.Petrole_et_gaz(), cls.Services_aux_consommateurs(), cls.Soins_de_sante() +
			cls.Technologie(), cls.Telecommunications(), cls.Utilitaires()]:
			data[results[0].sector] = [r.ticker for r in results]
		return data


class ResultSector:
	def __init__(self, data: dict):
		from . import OptionBourseDirect
		self.sector: str = data.get("industry")
		self.isin: str = data.get("isin")
		self.logo: str = data.get("logo")
		self.exchange: dict = data.get("market")
		self.name: str = data.get("name")
		self.slug: str = data.get("slug")
		self.ticker: str = data.get("ticker")
		self.url: str = OptionBourseDirect.LINK + data.get("url")
