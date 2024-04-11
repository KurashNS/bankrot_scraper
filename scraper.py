import ua_generator
from aes.aes import compute_cookie
from urllib.parse import quote
from requests import Session, RequestException

from tenacity import retry, retry_if_exception_type, wait_random, stop_after_attempt

from bs4 import BeautifulSoup, ResultSet, PageElement
import re

from excel.xlsx_io import output_check_result, Person

import time
from datetime import datetime

from logging import Logger


class BankrotScraper:
	def __init__(self, logger: Logger):
		self._url = 'https://old.bankrot.fedresurs.ru/DebtorsSearch.aspx'
		self._headers = self._get_headers()
		self._bankrot_cookie = ''

		self._logger = logger

		self.output_file = f'excel/output/bankrot_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'

	@property
	def check_result_template(self) -> dict[str: str]:
		return {
			'Фамилия': '',
			'Имя': '',
			'Отчество': '',
			'Категория': '',
			'ИНН': '',
			'ОГРНИП': '',
			'СНИЛС': '',
			'Регион': '',
			'Адрес': '',
			'Статус проверки': ''
		}

	@staticmethod
	def _get_headers() -> dict[str: str]:
		ua = ua_generator.generate(device='desktop')
		return {
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
			'Accept-Language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
			'Cache-Control': 'max-age=0',
			'Connection': 'keep-alive',
			'Referer': 'https://old.bankrot.fedresurs.ru/DebtorsSearch.aspx',
			'Sec-Fetch-Dest': 'document',
			'Sec-Fetch-Mode': 'navigate',
			'Sec-Fetch-Site': 'same-origin',
			'Sec-Fetch-User': '?1',
			'Upgrade-Insecure-Requests': '1',
			"User-Agent": ua.text,
			'sec-ch-ua': ua.ch.brands,
			'sec-ch-ua-mobile': ua.ch.mobile,
			'sec-ch-ua-platform': ua.ch.platform,
		}

	def _create_session(self) -> Session:
		session = Session()

		session.headers.update(self._headers)
		session.cookies.update({'bankrotcookie': self._bankrot_cookie})
		session.proxies = {
			'http': 'socks5://yfy5n4:s4SsUv@185.82.126.71:13518',
			'https': 'socks5://yfy5n4:s4SsUv@185.82.126.71:13518'
		}

		return session

	def _make_check_request(self, debtor: Person) -> str:
		with self._create_session() as session:
			session.cookies.update({
				'debtorsearch': 'typeofsearch=Persons&'
				                'orgname=&'
				                'orgaddress=&'
				                'orgregionid=&'
				                'orgogrn=&'
				                'orginn=&'
				                'orgokpo=&'
				                'OrgCategory=&'
				                f'prslastname={quote(debtor.last_name)}&'
				                f'prsfirstname={quote(debtor.first_name)}&'
				                f'prsmiddlename={quote(debtor.middle_name)}&'
				                'prsaddress=&'
				                'prsregionid=&'
				                'prsinn=&'
				                'prsogrn=&'
				                'prssnils=&'
				                'PrsCategory=&'
				                'pagenumber=0'
			})
			with session.get(url=self._url) as check_response:
				check_response.raise_for_status()
				return check_response.text

	def _set_bankrot_cookie(self, check_response: str) -> None:
		a_hash_matches = re.findall(pattern=r'a=toNumbers\("([^"]+)"\)', string=check_response)
		a_hash = a_hash_matches[0] if a_hash_matches else None

		b_hash_matches = re.findall(pattern=r'b=toNumbers\("([^"]+)"\)', string=check_response)
		b_hash = b_hash_matches[0] if b_hash_matches else None

		c_hash_matches = re.findall(pattern=r'c=toNumbers\("([^"]+)"\)', string=check_response)
		c_hash = c_hash_matches[0] if c_hash_matches else None

		if a_hash and b_hash and c_hash:
			self._bankrot_cookie = compute_cookie(a_hash=a_hash, b_hash=b_hash, c_hash=c_hash, mode=2)
			self._logger.info(f'New bankrot cookie: {self._bankrot_cookie}')
		else:
			raise RequestException('No hashes in setting bankrot cookie script')

	def _process_debtor_data(self, debtor: Person, debtors_table_rows: ResultSet[PageElement]) -> dict[str: str]:
		debtor_data = self.check_result_template
		if len(debtors_table_rows) == 1:
			debtor_data['Фамилия'] = debtor.last_name
			debtor_data['Имя'] = debtor.first_name
			debtor_data['Отчество'] = debtor.middle_name
			debtor_data['Статус проверки'] = 'Не найдено'
		else:
			debtors_table_headers = debtors_table_rows[0].find_all(name='th')
			debtors_table_data = debtors_table_rows[1].find_all(name='td')
			for header, data_field in zip(debtors_table_headers, debtors_table_data):
				if header.text.strip().lower() == 'должник':
					debtor_name_parts = data_field.text.strip().split()
					debtor_data['Фамилия'] = debtor_name_parts[0].capitalize()
					debtor_data['Имя'] = debtor_name_parts[1].capitalize()
					debtor_data['Отчество'] = ' '.join(debtor_name_parts[2:]).capitalize() if len(debtor_name_parts) > 2 else ''
				else:
					debtor_data[header.text.strip()] = data_field.text.strip()
			debtor_data['Статус проверки'] = 'Успешно'

		self._logger.info(f'Check result ({debtor.full_name}): {debtor_data}')
		return debtor_data

	def _extract_debtor_data(self, debtor: Person, check_response_soup: BeautifulSoup) -> dict[str: str]:
		debtors_table = check_response_soup.find(name='table', attrs={'class': 'bank', 'id': 'ctl00_cphBody_gvDebtors'})
		if not debtors_table:
			raise ValueError('No debtors table found in check response')

		debtors_table_rows = debtors_table.find_all(name='tr')
		if debtors_table_rows:
			return self._process_debtor_data(debtor=debtor, debtors_table_rows=debtors_table_rows)
		else:
			raise ValueError('No rows found in debtors table')

	def _process_check_response(self, debtor: Person, check_response: str):
		check_response_html = BeautifulSoup(markup=check_response, features='html.parser')
		bankrot_cookie_script = check_response_html.find(name='script', attrs={'type': 'text/javascript', 'src': '/aes.min.js'})
		if bankrot_cookie_script:
			self._set_bankrot_cookie(check_response=check_response)
			raise RequestException('Setting new bankrot cookie')
		else:
			return self._extract_debtor_data(debtor=debtor, check_response_soup=check_response_html)

	@retry(retry=retry_if_exception_type(RequestException), sleep=time.sleep,
	       wait=wait_random(min=1, max=2), stop=stop_after_attempt(20), reraise=True)
	def check_debtor(self, debtor: Person):
		check_response = self._make_check_request(debtor=debtor)
		check_result = self._process_check_response(debtor=debtor, check_response=check_response)
		return check_result


if __name__ == '__main__':
	debtor_ = Person()
	debtor_.last_name = 'Иванов'
	debtor_.first_name = 'Иван'
	debtor_.middle_name = 'Иванович'

	scraper = BankrotScraper(logger=Logger(__name__))
	scraper.check_debtor(debtor=debtor)
