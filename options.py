
import requests
from bs4 import BeautifulSoup


class Options:
	# Mis à jour le 16.01.2024 (Google Chrome)
	HEADERS = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}

	@classmethod
	def requestJson(cls, url: str, params: dict = None) -> dict:
		resp = requests.get(url=url, headers=cls.HEADERS, params=params)
		return resp.json()

	@classmethod
	def requestGet(cls, url: str, params: dict = None):
		resp = requests.get(url=url, headers=cls.HEADERS, params=params)
		return BeautifulSoup(resp.content, features="html.parser")
