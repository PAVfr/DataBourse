from bourseDirect import BourseDirect
from rendementbourse import RendementBourse
from stockevents import StockEvents

import sqlite3
import pandas
import datetime

# sqlite3
path_sql = "db_bourse.sqlite3"
cnx = sqlite3.Connection(path_sql)


def exportCSV(name, index=False):
	pandas.read_sql_query(f"""SELECT * FROM "{name}" """, cnx).to_csv(f"bdd_{name}.csv", index=index, encoding="utf-8")


class Entreprise:
	name = "entreprise"

	def __init__(self):
		self.create_table()

	def create_table(self):
		cursor = cnx.cursor()
		cursor.execute(f"""
			CREATE TABLE IF NOT EXISTS "{self.name}" (
				"TICKER"	TEXT NOT NULL UNIQUE,
				"NAME"	TEXT NOT NULL UNIQUE,
				"SECTEUR"	TEXT,
				"PEA-PME"	INTEGER,
				"ISIN"	TEXT NOT NULL UNIQUE,
				"INDICE"	TEXT,
				"HREF_BOURSEDIRECT"	TEXT,
				"HREF_RENDEMENTBOURSE"	TEXT
			);""")
		cnx.commit()

	def update_all_enterprise(self):
		cursor = cnx.cursor()
		secteurs = RendementBourse.sector.all_sector()
		peapme = [v.ticker for v in BourseDirect.PEA_PME()]
		tickers = []

		for indice, lines in {
			"CAC_40": BourseDirect.CAC_40(),
			"CAC_NEXT_20": BourseDirect.CAC_NEXT_20(),
			"CAC_MID_60": BourseDirect.CAC_MID_60(),
			"CAC_SMALL": BourseDirect.CAC_SMALL()
			}.items():

			for line in lines:
				# Ignore les entreprises en Europe
				if not line.isin.startswith("FR"):
					continue
				# PEA-PME
				PEAPME = 0 if line.ticker not in peapme else 1
				# SECTEUR & HREF_RENDEMENTBOURSE
				secteur = None
				href_rendementbourse = None
				for keys, lines in secteurs.items():
					for value in lines:
						value: ResultSectorRDM
						if line.ticker == value.ticker:
							secteur = value.sector
							href_rendementbourse = value.href
							break
				# TICKERS - Ajout dans la liste "tickers"
				tickers.append(line.ticker)
				# DONNEES
				cursor.execute(f"""SELECT * FROM {self.name} WHERE "TICKER"="{line.ticker}" """)
				if cursor.fetchone() is None:  # Ajoute la ligne si n'existe pas, sinon met à jour les données
					cursor.execute(f"""
						INSERT INTO {self.name} ("TICKER", "NAME", "SECTEUR", "PEA-PME", "ISIN", "INDICE", "HREF_BOURSEDIRECT", "HREF_RENDEMENTBOURSE") 
						VALUES("{line.ticker}", "{line.name}", "{secteur}", {PEAPME}, "{line.isin}", "{indice}", "{line.slug}", "{href_rendementbourse}")""")
					cnx.commit()
				else:
					cursor.execute(f"""
						UPDATE "{self.name}" 
						SET "ISIN"="{line.isin}", "SECTEUR"="{secteur}", "PEA-PME"={PEAPME}, "INDICE"="{indice}", "HREF_BOURSEDIRECT"="{line.slug}", "HREF_RENDEMENTBOURSE"="{href_rendementbourse}" 
						WHERE "TICKER"="{line.ticker}"
						""")
					cnx.commit()
		# CHECKLIST TICKERS
		cursor.execute(f"""SELECT "TICKER" FROM "{self.name}" """)
		cursor.row_factory = lambda cursor, row: row[0]  # Manipulation pour ne pas avoir un Tuple en retour
		for ticker in cursor.fetchall():
			if ticker not in tickers:
				print("CHECKLIST TICKERS DELETE = ", ticker)
				# # recherche le "rowid" de la ligne
				# cursor.execute(f"""SELECT rowid, * FROM "{self.name}" WHERE "TICKER"="{ticker}" """)
				# # supprime la ligne
				# cursor.execute(f"""DELETE FROM "main"."{self.name}" WHERE _rowid_ IN ('{cursor.fetchone()}') """)
				# # cursor.execute(f"""DELETE FROM {self.name} WHERE rowid={cursor.fetchone()} """)


class Dividende:
	name = "dividende"

	def __init__(self):
		self.create_table()

	def create_table(self):
		cursor = cnx.cursor()
		cursor.execute(f"""
			CREATE TABLE IF NOT EXISTS "{self.name}" (
				"TICKER"	TEXT,
				"EX_DIVIDEND"	TEXT,
				"DATE_PAYEMENT"	TEXT,
				"VALUE" TEXT
			);""")
		cnx.commit()

	def update_all_dividend(self, ignore_future: bool = False):
		"""
		:param ignore_future: inscit ou non les dividendes future à aujourd'hui
		:return:
		"""
		cursor = cnx.cursor()
		cursor.execute(f"""SELECT "TICKER" FROM {Entreprise.name} ORDER BY "TICKER" ASC""")
		cursor.row_factory = lambda cursor, row: row[0]  # Manipulation pour ne pas avoir un Tuple en retour
		for ticker in cursor.fetchall():
			print(ticker)
			for row in StockEvents.dividend.dividend_history(ticker=ticker):
				ex_dividend = row.get('date_ex_dividend')
				date_payement = row.get('date_payement')
				value = row.get('value') if "," in row.get('value') else row.get('value') + ",0"

				# Ignore Future Dividende
				if ignore_future and datetime.date.today() < datetime.datetime.strptime(ex_dividend, "%Y-%m-%d").date():
					continue

				# Ajoute la ligne dans la BDD si elle n'existe pas
				if cursor.execute(
						f"""SELECT * FROM {self.name} WHERE "TICKER"="{ticker}" AND "EX_DIVIDEND"=date("{ex_dividend}") """).fetchone() is None:
					cursor.execute(f"""
						INSERT INTO {self.name} ("TICKER", "EX_DIVIDEND", "DATE_PAYEMENT", "VALUE")
						VALUES("{ticker}", "{ex_dividend}", "{date_payement}", "{value}")""")
					cnx.commit()


if __name__ == '__main__':
	enterprise = Entreprise()
	enterprise.update_all_enterprise()
	exportCSV(enterprise.name)

	dividende = Dividende()
	dividende.update_all_dividend(ignore_future=True)
	exportCSV(dividende.name)

	cnx.close()
