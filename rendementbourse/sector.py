
from options import Options

import re


class Sector:
	@classmethod
	def Consommation_de_base(cls):
		return cls._request("https://rendementbourse.com/action/consommation-base", "Consommation de base")

	@classmethod
	def Consommation_discretionnaire(cls):
		return cls._request("https://rendementbourse.com/action/consommation-discretionnaire", "Consommation discrétionnaire")

	@classmethod
	def Energie(cls):
		return cls._request("https://rendementbourse.com/action/energie", "Energie")

	@classmethod
	def Finance(cls):
		return cls._request("https://rendementbourse.com/action/finance", "Finance")

	@classmethod
	def Immobilier(cls):
		return cls._request("https://rendementbourse.com/action/immobilier", "Immobilier")

	@classmethod
	def Industrie(cls):
		return cls._request("https://rendementbourse.com/action/industrie", "Industrie")

	@classmethod
	def Materiaux(cls):
		return cls._request("https://rendementbourse.com/action/materiaux", "Matériaux")

	@classmethod
	def Sante(cls):
		return cls._request("https://rendementbourse.com/action/sante", "Santé")

	@classmethod
	def Services_aux_collectivites(cls):
		return cls._request("https://rendementbourse.com/action/collectivites", "Services aux collectivités")

	@classmethod
	def Technologies(cls):
		return cls._request("https://rendementbourse.com/action/technologies", "Technologies")

	@classmethod
	def Telecommunications(cls):
		return cls._request("https://rendementbourse.com/action/communication", "Télécommunications")

	@classmethod
	def all_sector(cls) -> dict[list]:
		# Création du dict vide par défaut
		data = {}
		# Réccupère la liste des méthodes à exécuter
		for method in [v for v in dir(Sector) if not v.startswith("_") and re.match(r"[A-Z]\w", v)]:
			secteur = None
			lines: list[ResultSectorRDM] = []
			# Execute les methodes
			for line in getattr(cls, method)():
				secteur = line.sector
				lines.append(line)
			data[secteur] = lines
		return data

	@classmethod
	def _request(cls, url: str, sector: str, params=None):
		lines = []
		soup = Options.requestGet(url=url, params=params)
		table = soup.select_one("#quotesList > tbody")
		for line in table.find_all("a"):
			# Si le ticker est bien un ".PA"
			code = line.text.strip().splitlines()[0]
			if code.endswith(".PA"):
				result = ResultSectorRDM()
				result.sector = sector
				result.code = code
				result.ticker = code.split(".", maxsplit=1)[0]
				result.name = line.text.strip().splitlines()[-1].strip()
				result.href = line.get("href")
				lines.append(result)
		return lines


class ResultSectorRDM:
	def __init__(self):
		self.sector: str = None
		self.name: str = None
		self.code: str = None
		self.ticker: str = None
		self.href: str = None


if __name__ == '__main__':
	ticker = "TFI"

	secteur = None
	href_rendementbourse = None

	for keys, lines in Sector.all_sector().items():
		for value in lines:
			value: ResultSectorRDM
			if ticker == value.ticker:
				secteur = value.sector
				href_rendementbourse = value.href
				break

	print(secteur)
	print(href_rendementbourse)
