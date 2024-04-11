from scraper import BankrotScraper
from excel.xlsx_io import get_debtors_list, output_check_result

from log import BankrotLogger

INPUT_FILE = 'excel/input/debtors_list.xlsx'


def main():
	logger = BankrotLogger()
	scraper = BankrotScraper(logger=logger)

	try:
		debtors_list = get_debtors_list(input_excel_file=INPUT_FILE)
	except Exception as e:
		logger.error(f'Error: {type(e)} - {e}')
		return

	for debtor in debtors_list:
		try:
			check_result = scraper.check_debtor(debtor=debtor)
		except Exception as e:
			logger.error(f'Error ({person.full_name}): {type(e)} - {e}')
			check_result = scraper.check_result_template
			check_result['Фамилия'] = debtor.last_name
			check_result['Имя'] = debtor.first_name
			check_result['Отчество'] = debtor.middle_name
			check_result['Статус проверки'] = 'Ошибка'
		finally:
			output_check_result(output_file=scraper.output_file, check_result=check_result)


if __name__ == '__main__':
	import time

	start_time = time.time()
	print('--------------------------- START ---------------------------')
	main()
	print('--------------------------- FINISH ---------------------------')
	print(f'Time: {time.time() - start_time}')
