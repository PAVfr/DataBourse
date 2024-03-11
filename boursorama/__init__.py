
from options import Options


class Bousorama:
	@classmethod
	def searchHREF(cls, isin: str, place="Euronext Paris"):
		soup = Options.requestGet(url=f"https://www.boursorama.com/recherche/ajax?query={isin}&searchId=")
		for result in soup.find_all("a"):
			p = result.find("p")
			if p is None:
				continue
			elif place == str(p.text).strip():
				return result.get("href").split("/", maxsplit=2)[-1]
		return None
