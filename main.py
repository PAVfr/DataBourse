
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
				"HREF_BOURSEDIRECT"	TEXT,
				"HREF_RENDEMENTBOURSE"	TEXT
			);""")
		cnx.commit()

	def update_all_enterprise(self):
		cursor = cnx.cursor()
		secteurs = RendementBourse.sector.all_sector()
		peapme = [v.ticker for v in BourseDirect.PEA_PME()]

		for line in BourseDirect.CAC_ALL_TRADABLE():
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
			# DONNEES
			cursor.execute(f"""SELECT * FROM {self.name} WHERE "TICKER"="{line.ticker}" """)
			if cursor.fetchone() is None:  # Ajoute la ligne si n'existe pas, sinon met à jour les données
				cursor.execute(f"""
					INSERT INTO {self.name} ("TICKER", "NAME", "SECTEUR", "PEA-PME", "ISIN", "HREF_BOURSEDIRECT", "HREF_RENDEMENTBOURSE") 
					VALUES("{line.ticker}", "{line.name}", "{secteur}", {PEAPME}, "{line.isin}", "{line.slug}", "{href_rendementbourse}")""")
				cnx.commit()
			else:
				cursor.execute(f"""
					UPDATE "{self.name}" 
					SET "ISIN"="{line.isin}", "SECTEUR"="{secteur}", "PEA-PME"={PEAPME}, "HREF_RENDEMENTBOURSE"="{href_rendementbourse}" 
					WHERE "TICKER"="{line.ticker}"
					""")
				cnx.commit()


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

	def update_all_dividend(self):
		cursor = cnx.cursor()
		cursor.execute(f"""SELECT "TICKER" FROM {Entreprise.name} ORDER BY "TICKER" ASC""")
		cursor.row_factory = lambda cursor, row: row[0]  # Manipulation pour ne pas avoir un Tuple en retour
		for ticker in cursor.fetchall():
			print(ticker)
			for row in StockEvents.dividend.dividend_history(ticker=ticker):
				ex_dividend = row.get('date_ex_dividend')
				date_payement = row.get('date_payement')
				value = row.get('value') if "," in row.get('value') else row.get('value') + ",0"

				# Ajoute la ligne dans la BDD si elle n'existe pas
				if cursor.execute(f"""SELECT * FROM {self.name} WHERE "TICKER"="{ticker}" AND "EX_DIVIDEND"=date("{ex_dividend}") """).fetchone() is None:
					cursor.execute(f"""
						INSERT INTO {self.name} ("TICKER", "EX_DIVIDEND", "DATE_PAYEMENT", "VALUE")
						VALUES("{ticker}", "{ex_dividend}", "{date_payement}", "{value}")""")
					cnx.commit()


if __name__ == '__main__':
	enterprise = Entreprise()
	# enterprise.update_all_enterprise()
	exportCSV(enterprise.name)

	dividende = Dividende()
	dividende.update_all_dividend()
	exportCSV(dividende.name)

	cnx.close()
