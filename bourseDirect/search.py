
from options import Options

import math


class Search:
	@classmethod
	def searchV3(cls, params: dict) -> list[dict]:
		"""Retourne la liste des entreprises éligibles au PEA-PME en France"""
		instruments = []
		page = 0
		pagemax = 1

		while page < pagemax:
			params.update({"nature": "stock",
						   "country": "France",
						   "currency": "Euro",
						   "page": str(page), "size": "60"})
			data = Options.requestJson(url="https://www.boursedirect.fr/api/instrument/v3/search", params=params)
			pagemax = math.ceil(int(data.get("count")) / int(
				params["size"]))  # Définit le nombre de pages max, suivant le nombre de résultats
			for v in data.get("instruments"):
				instruments.append(v)
			page += 1
		return instruments
