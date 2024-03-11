
from .dividend import Dividend


class StockEvents:
	dividend = Dividend


if __name__ == '__main__':
	href = "/fr/event/dividends/33417389"
	div = Dividend.dividend_history()
	# div = Dividend.getInfos(href=href)
	print(div)
