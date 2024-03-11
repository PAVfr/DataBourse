
from options import Options

import re
import datetime


class Dividend:
	URL = "https://stockevents.app"

	@classmethod
	def dividend_history(cls, ticker: str):
		soup = Options.requestGet(f"{cls.URL}/fr/stock/{ticker}.PA")
		lines = [element for element in soup.find_all("a", href=True) if element.get("href").startswith("/fr/event/dividends/") and element.get("target") is None]
		if len(lines) == 0:
			return []
		else:
			return [{**{
				"ticker": ticker,
				"href": line.get("href"),
				"value": line.find_all("span")[-1].text.replace(".", ",").replace("€", ""),
				}, **cls.getInfos(href=line.get("href"))} for line in lines]

	@classmethod
	def getInfos(cls, href: str):
		"""
		date_ex_dividend = date à partir de laquelle elle est cotée sans le droit au dividende à venir
		date_payement = date de payement du dividende
		"""
		soup = Options.requestGet(f"{cls.URL}{href}")

		date_ex_dividend = soup.select_one("""#app > div > div.max-w-5xl.mx-auto.py-12.min-h-screen.px-2 > div > 
			div.col-span-3.md\:col-span-2 > div.relative > div:nth-child(3) > div > div.flex.flex-col.text-left > 
			p""").text.split("•")[-1].strip()

		date_paye = soup.select_one("""#app > div > div.max-w-5xl.mx-auto.py-12.min-h-screen.px-2 > div > 
			div.col-span-3.md\:col-span-2 > div.relative > div.flex > div.flex.flex-col.text-left > 
			p""").text.split("•")[-1].strip()

		try:
			date_paye = cls._format_date(date_paye)
		except ValueError:
			date_paye = "N/A"

		return {
			"date_ex_dividend": cls._format_date(date_ex_dividend),
			"date_payement": date_paye
			}

	@classmethod
	def _format_date(cls, txt: str):
		txt = txt.replace(",", "")
		# MOIS
		data = {
			"janvier": "01",
			"février": "02",
			"mars": "03",
			"avril": "04",
			"mai": "05",
			"juin": "06",
			"juillet": "07",
			"août": "08",
			"septembre": "09",
			"octobre": "10",
			"novembre": "11",
			"décembre": "12"
			}
		mois = txt.split(" ", maxsplit=1)[0]
		for k, v in data.items():
			mois = mois.replace(k, v)
		# Date
		jour = txt.split(" ")[1]
		annee = txt.split(" ")[-1]
		return datetime.datetime.strptime(f"{jour}/{mois}/{annee}", "%d/%m/%y").strftime("%Y-%m-%d")


if __name__ == '__main__':
	div = Dividend.getInfos("/fr/event/dividends/33629838")
	print(div)
