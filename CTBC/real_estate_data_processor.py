from selenium import webdriver
from selenium.webdriver.support.ui import Select
import os
from pathlib import Path
import pandas as pd
import time
import schedule
from zipfile import ZipFile
import re
import sqlite3

# Load environment variables from .env file
import os
from dotenv import load_dotenv
load_dotenv()


class Real_estate_data_processor:
    """Class to handle downloading, processing, and inserting real estate data into the database."""
    
    def __init__(self, chrome_driver_path, source_url, zip_data_path, db_path='db.sqlite3'):
        self.chrome_driver_path = chrome_driver_path
        self.source_url = source_url
        self.zip_data_path = zip_data_path
        self.db_path = db_path

    def download_by_selenium(self):
        """Downloads real estate data using Selenium."""
        try:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_experimental_option('prefs', {'download.default_directory': self.zip_data_path})
            browser = webdriver.Chrome(executable_path=self.chrome_driver_path, options=chrome_options)
            browser.get(self.source_url)
            time.sleep(0.5)
            Select(browser.find_element_by_name("fileFormat")).select_by_value('csv')
            time.sleep(0.5)
            browser.find_element_by_name("button9").click()
            time.sleep(3)
        except Exception as e:
            print(f"Error during download: {e}")
        finally:
            browser.close()
        return 'ok'

    def extract_and_process_csv(self, zip_obj, f_name):
        """Extracts CSV from ZIP file and processes it."""
        zip_obj.extract(f_name, self.zip_data_path)
        time.sleep(0.5)
        df = pd.read_csv(
            f'{self.zip_data_path}{f_name}',
            names=[
                "鄉鎮市區", "交易標的", "address", "土地移轉總面積平方公尺",
                "都市土地使用分區", "非都市土地使用分區", "非都市土地使用編定",
                "transaction_date_row", "交易筆棟數", "shifting_floor_number",
                "total_floor_number", "building_state", "usage", "主要建材",
                "build_date", "shifting_area", "建物現況格局-房", "建物現況格局-廳",
                "建物現況格局-衛", "建物現況格局-隔間", "有無管理組織",
                "sale_price", "單價元平方公尺", "車位類別", "車位移轉總面積(平方公尺)",
                "車位總價元", "notes", "serial_number", "主建物面積",
                "附屬建物面積", "陽台面積", "電梯", "移轉編號"
            ],
            usecols=[
                'address', 'transaction_date_row', 'sale_price', 'shifting_area',
                'building_state', 'usage', 'serial_number', 'total_floor_number',
                'shifting_floor_number', 'build_date', 'notes'
            ],
            converters={
                'shifting_area': str, 'transaction_date_row': str, 'total_floor_number': str,
                'shifting_floor_number': str, 'sale_price': str, 'build_date': str
            },
            encoding='utf-8'
        )
        return df

    def unzip_to_db(self):
        """Unzips downloaded files and inserts data into the database."""
        zip_name_list = os.listdir(self.zip_data_path)
        for zip_name in zip_name_list:
            with ZipFile(f'{self.zip_data_path}{zip_name}', 'r') as zip_obj:
                for name in zip_obj.namelist():
                    if re.match("^[a-z]_lvr_land_a.csv", name):
                        df = self.extract_and_process_csv(zip_obj, name)
                        with sqlite3.connect(self.db_path) as conn:
                            df.to_sql('core_app_real_estate_raw', conn, if_exists='append', index=False)
                        os.remove(f'{self.zip_data_path}{name}')
            os.remove(f'{self.zip_data_path}{zip_name}')
        self.clean_db()

    def clean_db(self):
        """Cleans the database by removing duplicates and unnecessary rows."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                DELETE FROM core_app_real_estate_raw 
                WHERE rowid NOT IN (
                    SELECT MIN(rowid) FROM core_app_real_estate_raw 
                    GROUP BY serial_number, address
                );
            ''')
            conn.execute('''
                DELETE FROM core_app_real_estate_raw
                WHERE transaction_date_row IN ('transaction year month and day', '交易年月日');
            ''')
            conn.execute('''
                UPDATE core_app_real_estate_raw
                SET transaction_date_row = CAST(transaction_date_row AS INT) + 19110000
                WHERE LENGTH(transaction_date_row) = 7;
            ''')
            conn.execute('''
                UPDATE core_app_real_estate_raw
                SET transaction_date_row = SUBSTR(transaction_date_row, 1, 4) || '-' || SUBSTR(transaction_date_row, 5, 2) || '-' || SUBSTR(transaction_date_row, 7, 2)
                WHERE LENGTH(transaction_date_row) = 8;
            ''')
            conn.execute('''
                UPDATE core_app_real_estate_raw
                SET transaction_date = transaction_date_row
                WHERE transaction_date IS NULL;
            ''')
            conn.commit()

    def run_scheduler(self):
        """Schedules the daily download and processing tasks."""
        schedule.every().day.at("05:00:00").do(self.download_by_selenium)
        schedule.every().day.at("05:00:10").do(self.unzip_to_db)
        while True:
            schedule.run_pending()
            time.sleep(1)
            print('<Real estate auto_downloader is running... The process will start at 05:00:00>')


def main():
    processor = Real_estate_data_processor(
        chrome_driver_path=f"{Path(__file__).resolve().parent}/chromedriver.exe",
        source_url='https://plvr.land.moi.gov.tw/DownloadOpenData',
        zip_data_path=os.environ.get('ZIP_DATA_PATH')
    )
    processor.download_by_selenium()
    time.sleep(10)


if __name__ == '__main__':
    main()
