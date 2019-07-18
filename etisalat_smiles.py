from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import ElementClickInterceptedException, ElementNotInteractableException
import time
import sqlite
import os

class Reward:
    pass

URL = 'https://smiles.etisalat.ae'

REWARD_ORIGIN = 'Etisalat Smiles'

REWARD_ORIGIN_LOGO = 'https://seeklogo.com/images/E/Etisalat-logo-A58AC10542-seeklogo.com.png'

VIEW_ALL_BUTTON_CSS_SELECTOR = 'dealsbycategory .config-section-title-wrapper a'
VIEW_ALL_PAGE_LOADED_CSS_SELECTOR = '.deal-info'
LOAD_MORE_BUTTON_CSS_SELECTOR = '.loadmore a'
SURVEY_POPUP_CSS_SELECTOR = '.survey-popup-buttons button'
REWARD_DETAILS_BUTTON_CSS_SELECTOR = '.deal-price__button a'
REWARD_DETAILS_CSS_SELECTORS = {
    'Background Image': '.deal-teaser__image img',
    'Logo': '.brand-logo img',
    'Company Name': '.deal-info__title',
    'Offer': '.deal-info__description',
    'Offer Type' : '.deal-info__tag',
    'Cost': '.deal-price__value'
}

options = Options()
options.headless = True
# bot = webdriver.Firefox(options = options)

bot = webdriver.Firefox()
bot.get(URL)
time.sleep(1)
try:
    time.sleep(1)
    dismiss_popup_button = bot.find_element(By.CSS_SELECTOR, SURVEY_POPUP_CSS_SELECTOR)
    dismiss_popup_button.click()
    time.sleep(1)
except:
    print('no popup found')
view_all_button = bot.find_element(
    By.CSS_SELECTOR, VIEW_ALL_BUTTON_CSS_SELECTOR)
view_all_button.click()
WebDriverWait(bot, 20).until(EC.presence_of_element_located(
    (By.CSS_SELECTOR, VIEW_ALL_PAGE_LOADED_CSS_SELECTOR)))
time.sleep(2)

while True:
    try:
        load_more_button = bot.find_element(By.CSS_SELECTOR, LOAD_MORE_BUTTON_CSS_SELECTOR)
        load_more_button.click()
        time.sleep(1)
    except ElementClickInterceptedException:
        try:
            time.sleep(1)
            dismiss_popup_button = bot.find_element(By.CSS_SELECTOR, SURVEY_POPUP_CSS_SELECTOR)
            dismiss_popup_button.click()
            time.sleep(1)
        except Exception as e:
            print(e)
    except ElementNotInteractableException:
        print('Done')
        break

results = {}
for name, cssSelector in REWARD_DETAILS_CSS_SELECTORS.items():
    try:
        results[name] = bot.find_elements(By.CSS_SELECTOR, cssSelector)
    except Exception as e:
        print(e)

results_sorted = []
logo_idx = 0

for idx, _ in enumerate(results['Company Name']):
    tmp = Reward()
    tmp.backgroundImage = results['Background Image'][idx].get_attribute('src')
    tmp.offerType = results['Offer Type'][idx].text.strip()
    if  tmp.offerType == 'ETISALAT BUNDLE':
        tmp.logo = REWARD_ORIGIN_LOGO
    else:
        tmp.logo = results['Logo'][logo_idx].get_attribute('src')
        logo_idx += 1
    tmp.companyName = results['Company Name'][idx].text
    tmp.offer = results['Offer'][idx].text
    tmp.cost = results['Cost'][idx].text
    results_sorted.append(tmp)

sqlConn = sqlite.SQLConnector()

if len(sqlConn.select_by_reward_origin(REWARD_ORIGIN)) > 0:
    sqlConn.delete_by_reward_origin(REWARD_ORIGIN)

INSERT_STATEMENT = """INSERT INTO rewards(
        reward_origin, 
        reward_origin_logo, 
        background_image, 
        logo, 
        offer, 
        offer_type, 
        company_name, 
        cost
    ) values (
        ?,?,?,?,?,?,?,?
    )"""
for i in results_sorted:
    sqlConn.insert_reward(
        INSERT_STATEMENT,
        (
            REWARD_ORIGIN,
            REWARD_ORIGIN_LOGO,
            i.backgroundImage,
            i.logo,
            i.offer,
            i.offerType,
            i.companyName,
            i.cost
        )
    )

print('{} successfully updated'.format(REWARD_ORIGIN))
bot.quit()