from seleniumbase import SB
from datetime import date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import os
import json
import pandas as pd
import creds #used to hide credentials 
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas


def extract_and_rename_file():
    with SB(uc=True, headed=True) as driver:
        #open link to sign into Google, type in email, and click sign in button 
        driver.get("https://accounts.google.com/o/oauth2/v2/auth/oauthchooseaccount?redirect_uri=https%3A%2F%2Fdevelopers.google.com%2Foauthplayground&prompt=consent&response_type=code&client_id=407408718192.apps.googleusercontent.com&scope=email&access_type=offline&flowName=GeneralOAuthFlow")
        driver.type("#identifierId", creds.email)
        driver.click("#identifierNext > div > button")

        #type in password, click sign in button
        driver.type("#password > div.aCsJod.oJeWuf > div > div.Xb9hP > input", creds.password)
        driver.click("#passwordNext > div > button")
        # open keybr.com, navigate to profile page that has download button
        driver.get("https://www.keybr.com/account")
        driver.click(".q0cDWjdhfy")
        driver.click('//a[contains(@href,"/profile")]')

        # Set the download directory and disable the "save as" prompt
        chrome_options = webdriver.ChromeOptions()
        download_dir = os.path.abspath("/Users/alex/Documents/DSProjects/keybr_project/downloaded_files")
        os.makedirs(download_dir, exist_ok=True)
        prefs = {"download.default_directory": download_dir,
                "download.directory_upgrade": True,
                "download.prompt_for_download": False,
                "download.extensions_to_open": ""}
        chrome_options.add_experimental_option("prefs", prefs)


        # Navigate to the download link and click the download button
        download_button = driver.driver.find_element(By.CSS_SELECTOR, 'button.q0cDWjdhfy[title="Download all your typing data in JSON format."]')
        download_button.click()

        # Wait for the file to be downloaded
        wait = WebDriverWait(driver.driver, 20)
        download_path = os.path.join(download_dir, "typing-data.json")
        wait.until(lambda driver: os.path.exists(download_path))

        today = date.today()
        current_date = today.strftime("%Y-%m-%d")
        file_path = "./downloaded_files/"
        file_name = "typing-data.json"

        # Construct the new file name
        new_file_name = f"{file_path}{current_date}_{file_name}"

        # Rename the file
        os.rename(file_path + file_name, new_file_name)

        return new_file_name


def parse_json_and_move(filename): 
    with open(filename,'r') as f:
        data = json.loads(f.read())

    # Flatten data
    df_nested_list = pd.json_normalize(data, meta = ["layout", "lessonType", "timeStamp", "length", "time", "errors", "speed"],record_path =['histogram'])
    cols = df_nested_list.columns.tolist()

    cols = cols[-7:] + cols[:-7]
    df_nested_list = df_nested_list[cols]

    filename_split = filename.split("/")
    filename_only = filename_split[2][:-5]
    
    os.rename(filename, f"./downloaded_files/archives/{filename_only}.json")
    df_nested_list.to_csv(f"./csv_dfs/{filename_only}.csv", index = False)
    return f"./csv_dfs/{filename_only}.csv"

def load_csv_into_sf(filename):
    conn = snow.connect(
        user = creds.SF_USER,
        password = creds.SF_PASS,
        account = creds.SF_ACCOUNT,
        warehouse = creds.SF_WAREHOUSE,
        database = "keybr",
        schema = "typing_data"
    )

    cur = conn.cursor()

    sql = '''create table if not exists keybr_lessons(
            LAYOUT VARCHAR,
            LESSONTYPE VARCHAR,
            TIMESTAMP timestamp_ntz,
            LENGTH NUMBER,
            TIME NUMBER,
            ERRORS NUMBER,
            SPEED NUMBER,
            CODEPOINT NUMBER,
            HITCOUNT NUMBER,
            MISSCOUNT NUMBER,
            TIMETOTYPE NUMBER,
            LOAD_DATE DATE)
        '''

    cur.execute(sql)

    sql = 'truncate table if exists keybr_lessons'
    cur.execute(sql)

    table = pd.read_csv('csv_dfs/2023-05-13_typing-data.csv', sep = ",")
    table.columns = [c.upper() for c in table.columns]
    today = date.today()
    current_date = today.strftime("%Y-%m-%d")
    table['LOAD_DATE'] = current_date
    write_pandas(conn, table, 'KEYBR_LESSONS')

    # Execute a statement that will turn the warehouse off.
    sql = f'ALTER WAREHOUSE {creds.SF_WAREHOUSE} SUSPEND'
    cur.execute(sql)

    cur.close()
    conn.close()


filename = extract_and_rename_file()
filename = parse_json_and_move(filename)
load_csv_into_sf(filename)
