import os
import sqlite3
import time
from collections import namedtuple
from typing import Tuple, NamedTuple

from art import tprint
from colorama import init, Fore
from loguru import logger
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ex_cs
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

init()
LOG_FMT = '{time:DD-MM-YYYY at HH:mm:ss} | {level: <8} | func: {function: ^15} | line: {line: >3} | message: {message}'
logger.add(sink='logs/debug.log', format=LOG_FMT, level='INFO', diagnose=True, backtrace=False,
           rotation="100 MB", retention=2, compression="zip")


class ConfigData:
    timeout = 1
    sql_file = os.path.abspath('WBSellersBase.db')
    dev = 'https://github.com/MalakhovStas'
    HOST = 'https://www.wildberries.ru'
    url = '/seller/'

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(
        'user-agent = Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36')
    chrome_options.add_argument(
        'accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/'
        'webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--remote-debugging-port=9222')
    chrome_options.add_argument('--enable-javascript')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-insecure-localhost')


class Database:
    """Класс описывающий работу приложения с базой данных"""

    """
    - ИД селлера
    - Бренд магазина
    - Данные юр лица
    - Наименование
    - Адрес (если есть)
    - ОГРН/ОГРНИП
    - Сколько продает на WB
    - Сколько проданных товаров
    - Категории через запятую
    - Бренды через запятую
    """

    def __init__(self):
        self.database = sqlite3.connect(ConfigData.sql_file)
        self.cursor = self.database.cursor()

    def create_table(self) -> None:
        with self.database:
            self.cursor.execute("""CREATE TABLE IF NOT EXISTS sellers(
                                    seller_id INTEGER PRIMARY KEY NOT NULL,
                                    brand_shop TEXT,
                                    company_name TEXT,
                                    company_address TEXT,
                                    company_ogrn TEXT,
                                    how_long_selling TEXT,
                                    how_many_items_sold TEXT,
                                    categories TEXT, 
                                    brands TEXT)""")
            self.database.commit()
        # logger.debug(f'\n-> OK -> CREATE TABLE "sellers" IF NOT EXISTS in database "{ConfigData.sql_file}"\n')

    def insert_seller(self, seller_id: int, brand_shop: str, company_name: str, company_address: str, company_ogrn: str,
                      how_long_selling: str, how_many_items_sold: str, categories: str, brands: str) -> None:
        with self.database:
            self.cursor.execute(
                "INSERT INTO sellers(seller_id, brand_shop, company_name, company_address, company_ogrn, "
                "how_long_selling, how_many_items_sold, categories, brands) VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?)", (seller_id, brand_shop, company_name, company_address,
                                                company_ogrn, how_long_selling, how_many_items_sold,
                                                categories, brands))
            self.database.commit()
        # logger.debug(f'-> OK -> INSERT new seller -> seller_id: {seller_id} brand_shop: {brand_shop}\n')

    def select_seller(self, seller_id: int) -> NamedTuple:
        seller = namedtuple('seller', ['seller_id', 'brand_shop', 'company_name', 'company_address', 'company_ogrn',
                                       'how_long_selling', 'how_many_items_sold', 'categories', 'brands'])
        with self.database:
            self.cursor.execute("SELECT seller_id, brand_shop, company_name, company_address, company_ogrn, "
                                "how_long_selling, how_many_items_sold, categories, brands FROM sellers "
                                "WHERE seller_id = ?", (seller_id,))
            data = self.cursor.fetchone()
        if data:
            seller = seller(*data)
        else:
            seller = None
        # logger.debug(f'-> OK -> SELECT seller -> return -> {seller}\n')
        return seller

    def count_all_sellers(self) -> int:
        with self.database:
            self.cursor.execute("SELECT COUNT(seller_id) FROM sellers")
            sellers = self.cursor.fetchone()
        if not sellers:
            logger.error(f'-> BAD -> NOT sellers in database -> return -> 0\n')
            return 0
        else:
            # logger.debug(f'-> OK -> COUNT all sellers -> return -> {sellers}\n')
            return sellers[0]


class MiscUtils:
    """Фронтенд инструменты"""

    @staticmethod
    def end_work(result: str) -> None:
        time.sleep(0.5)

        if result == 'BAD':
            print(f'\n{Fore.RED}Парсинг данных с сайта {ConfigData.HOST} завершился критической ошибкой, '
                  f'данные получены не в полном объёме{Fore.RESET}')

        else:
            print(f'\n{Fore.GREEN}Парсинг данных с сайта {ConfigData.HOST} '
                  f'выполнен корректно, данные получены в полном объёме', Fore.RESET)

        time.sleep(1)
        print(f'\n{Fore.GREEN}Разработано: {Fore.BLUE}{ConfigData.dev}', Fore.RESET)
        input(f'{Fore.YELLOW}Для завершения работы нажмите -{Fore.RESET} Enter')
        time.sleep(1)

    @staticmethod
    def choice_next_stage() -> str:
        while True:
            time.sleep(0.5)
            next_stage = input(f'\n{Fore.YELLOW}Продолжить парсинг{Fore.RESET} - y / n ').lower()
            if next_stage == 'y':
                stage = 'next'
                break
            elif next_stage == 'n':
                stage = 'stop'
                break
            time.sleep(0.5)
            print(f'{Fore.RED}Ошибка ввода, нужно ввести - y или - n', Fore.RESET)
        return stage

    @staticmethod
    def get_range_sellers_id() -> Tuple:
        start_id, stop_id = 'start', 'start'
        while isinstance(start_id, str) or isinstance(stop_id, str):
            start_id = input(f"\n{Fore.YELLOW}Введите начальный seller_id: "
                             f"{Fore.RESET} ").strip()

            stop_id = input(f"\n{Fore.YELLOW}Введите конечный seller_id: "
                            f"{Fore.RESET} ").strip()
            start_id = int(start_id) if start_id.isdigit() else 'restart'
            stop_id = int(stop_id) if stop_id.isdigit() else 'restart'

        return start_id, stop_id


class ParseUtils:
    """Инструменты парсинга"""

    @staticmethod
    def wait_load(driver, wait, seller_id: int) -> None:
        try:
            driver.get(ConfigData.HOST + ConfigData.url + str(seller_id))
            wait.until(ex_cs.visibility_of_element_located((By.CSS_SELECTOR, '#catalog')))
            left = driver.find_element(By.CSS_SELECTOR, '#catalog > div.catalog-page__side')
            if left.aria_role != 'none':
                wait.until(
                    ex_cs.visibility_of_element_located((By.CSS_SELECTOR, '#catalog > div.catalog-page__side')))
                wait.until(
                    ex_cs.visibility_of_element_located((By.CSS_SELECTOR, '#catalog > div.catalog-page__seller-details '
                                                                          '> div.seller-details > div.seller-details__'
                                                                          'parameter-wrap > div')))
        except NoSuchElementException:
            pass
        except TimeoutException:
            pass

    @staticmethod
    def get_brand_shop(driver) -> str:
        try:
            brand_shop = driver.find_element(
                By.XPATH, '//*[@id="catalog"]/div[4]/div[1]/div[1]/div[2]/div[1]/h2').text.strip()
        except NoSuchElementException:
            brand_shop = None
        except TimeoutException:
            brand_shop = None
        return brand_shop

    @staticmethod
    def get_company_info(driver, wait) -> Tuple[str, str, str]:
        try:
            but_info = driver.find_element(
                By.XPATH, '/html/body/div[1]/main/div[2]/div/div[2]/div/div[4]/div[1]/div[1]/div[2]/div[1]/span')
            but_info.click()
            wait.until(ex_cs.visibility_of_element_located((By.XPATH, '/html/body/div[7]/div')))
            data_face = driver.find_element(By.XPATH, '/html/body/div[7]/div').text.split('\n')
            company_name, company_address, company_ogrn = 'нет информации', 'нет информации', 'нет информации'
            for index, i_data in enumerate(data_face):
                if index == 0:
                    company_name = i_data
                elif i_data.startswith('ОГРН:'):
                    company_ogrn = i_data.lstrip('ОГРН:').strip()
                else:
                    company_address = i_data
        except NoSuchElementException:
            company_name, company_address, company_ogrn = 'нет информации', 'нет информации', 'нет информации'
        except TimeoutException:
            company_name, company_address, company_ogrn = 'нет информации', 'нет информации', 'нет информации'
        return company_name, company_address, company_ogrn

    @staticmethod
    def get_how_long_selling(driver, wait) -> str:
        try:
            wait.until(ex_cs.visibility_of_element_located
                       ((By.XPATH, '/html/body/div[1]/main/div[2]/div/div[2]/div/div[4]/div[1]'
                                   '/div[2]/div/div[1]/p[1]')))
            how_long_selling = driver.find_element(
                By.XPATH, '/html/body/div[1]/main/div[2]/div/div[2]/div/div[4]/div[1]/div[2]/div/div[1]/p[1]').text

        except NoSuchElementException:
            how_long_selling = 'нет информации'
        except TimeoutException:
            how_long_selling = 'нет информации'

        return how_long_selling

    @staticmethod
    def get_how_many_items_sold(driver, wait) -> str:
        try:
            wait.until(ex_cs.visibility_of_element_located(
                (By.XPATH, '/html/body/div[1]/main/div[2]/div/div[2]/div/div[4]/div[1]/div[2]/div/div[2]/p[1]')))
            how_many_items_sold = driver.find_element(
                By.XPATH, '/html/body/div[1]/main/div[2]/div/div[2]/div/div[4]/div[1]/div[2]/div/div[2]/p[1]').text
        except NoSuchElementException:
            how_many_items_sold = 'нет информации'
        except TimeoutException:
            how_many_items_sold = 'нет информации'
        else:
            if '%' in how_many_items_sold:
                how_many_items_sold = 'нет информации'

        return how_many_items_sold

    @staticmethod
    def get_categories(driver) -> str:
        try:

            categories = driver.find_element(
                By.XPATH, '/html/body/div[1]/main/div[2]/div/div[2]/div/div[5]/div[1]/div/ul/li/ul').text
            categories = ', '.join(categories.split('\n'))
        except NoSuchElementException:
            categories = 'нет информации'
        except TimeoutException:
            categories = 'нет информации'

        return categories

    @staticmethod
    def get_brands(driver) -> str:
        try:
            brands = []
            for brand in driver.find_elements(By.CSS_SELECTOR, '#filters > div.j-filter-container.filter.filter'
                                                               'block.render_type_6.fbrand.show.filter-active > '
                                                               'div.j-filter-content.filter__content > fieldset '
                                                               '> label'):
                brands.append(brand.text)

        except NoSuchElementException:
            brands = 'нет информации'
        except TimeoutException:
            brands = 'нет информации'
        else:
            if not brands:
                brands = 'нет информации'
            else:
                brands = ', '.join(brands)

        return brands


class ParseStream:

    @staticmethod
    def func_stream(db, start_id: int, stop_id: int) -> Tuple[int, int]:

        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager(cache_valid_range=1).install()),
                                  chrome_options=ConfigData.chrome_options)
        wait = WebDriverWait(driver, ConfigData.timeout)
        connection = ParseUtils

        add_seller, num_pages = 0, 0
        for num_pages, seller_id in enumerate(range(start_id, stop_id + 1)):

            if not db.select_seller(seller_id) is None:
                logger.warning(f'Селлер id: {seller_id} есть в базе -> пропускаю')
                continue

            connection.wait_load(driver, wait, seller_id)

            brand_shop = connection.get_brand_shop(driver)
            if brand_shop is None or brand_shop == 'Продавец':
                logger.warning(f' Пустой селлер -> id: {seller_id}')
                continue

            company_name, company_address, company_ogrn = connection.get_company_info(driver, wait)
            how_long_selling = connection.get_how_long_selling(driver, wait)
            how_many_items_sold = connection.get_how_many_items_sold(driver, wait)
            categories = connection.get_categories(driver)
            brands = connection.get_brands(driver)

            db.insert_seller(seller_id, brand_shop, company_name, company_address, company_ogrn, how_long_selling,
                             how_many_items_sold, categories, brands)
            logger.debug(f'Добавлен селлер:\n    id: {seller_id}\n    бренд: {brand_shop}'
                         f'\n    название компании: {company_name}\n    адрес: {company_address}'
                         f'\n    ОГРН: {company_ogrn}\n    продаёт на WB: {how_long_selling}'
                         f'\n    продано товаров: {how_many_items_sold}'
                         f'\n    категории: {categories}\n    бренды: {brands}')
            add_seller += 1
        driver.quit()
        return add_seller, num_pages


class Parser:

    @staticmethod
    def parser() -> None:
        print(f'{Fore.GREEN}Разработано: {Fore.BLUE}{ConfigData.dev}', Fore.RESET)
        stage = 'start'
        tprint('WBSellersParser')
        time.sleep(0.5)

        db = Database()
        db.create_table()

        add_seller, num_pages, start_id, stop_id = 0, 0, 0, 0
        try:
            while stage != 'stop':
                start_id, stop_id = MiscUtils.get_range_sellers_id()
                logger.info(f'Начинаю парсинг, в базе {db.count_all_sellers()} селлеров')

                add_seller, num_pages = ParseStream.func_stream(db, start_id, stop_id)

                logger.info(f'Проверено селлеров: {num_pages}, добавлено селлеров: {add_seller}')
                logger.info(f'Парсинг окончен в базе {db.count_all_sellers()} селлеров')
                stage = MiscUtils.choice_next_stage()

        except Exception as exc:
            logger.error(f'Критическая ошибка: {exc}\n')
            logger.warning(f'Парсинг выбранного диапазона {start_id} - {stop_id} закончен не полностью')

            MiscUtils.end_work(result='Bad')
        else:
            MiscUtils.end_work(result='Ok')


if __name__ == '__main__':
    os.system("mode con cols=200 lines=40")
    Parser.parser()
