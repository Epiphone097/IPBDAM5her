from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json


def convert_currency(base, to, amount):
	"""
	Converts a currency to target currency
	:param base: Currency to be converted
	:param to: Currency to get the conversion result from
	:param amount: The amount to be converted
	:return: Float: Price of the amount to be converted
	"""
	url = ' https://pro-api.coinmarketcap.com/v1/tools/price-conversion'
	parameters = {
		'symbol': base,
		'convert': to,
		'amount': amount
	}

	# too lazy to secure key
	# 57e78373-0c60-4f4c-aa10-966bd4c9b970

	# Set headers, including API key
	headers = {
		'Accepts': 'application/json',
		'X-CMC_PRO_API_KEY': '57e78373-0c60-4f4c-aa10-966bd4c9b970',
	}
	session = Session()
	session.headers.update(headers)

	# Get and return the data
	try:
		response = session.get(url, params=parameters)
		data = json.loads(response.text)
		return data['data']['quote'][to]['price']
	except (ConnectionError, Timeout, TooManyRedirects) as e:
		print(e)