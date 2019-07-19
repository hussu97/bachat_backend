from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import ElementClickInterceptedException, ElementNotInteractableException
import time
import scraping_scripts.reward_object as reward

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
    'Offer Type': '.deal-info__tag',
    'Cost': '.deal-price__value'
}


class EtisalatSmiles:
    def __init__(self):
        # options = Options()
        # options.headless = True
        # bot = webdriver.Firefox(options = options)
        self.bot = webdriver.Firefox()
        self.results = self.run_script()
        print('{} successfully retrieved'.format(self.results[0].rewardOrigin))
        self.bot.quit()

    def run_script(self):
        self.bot.get(URL)
        time.sleep(1)
        try:
            time.sleep(1)
            dismiss_popup_button = self.bot.find_element(
                By.CSS_SELECTOR, SURVEY_POPUP_CSS_SELECTOR)
            dismiss_popup_button.click()
            time.sleep(1)
        except:
            print('no popup found')
        view_all_button = self.bot.find_element(
            By.CSS_SELECTOR, VIEW_ALL_BUTTON_CSS_SELECTOR)
        view_all_button.click()
        WebDriverWait(self.bot, 20).until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, VIEW_ALL_PAGE_LOADED_CSS_SELECTOR)))
        time.sleep(2)

        while True:
            try:
                load_more_button = self.bot.find_element(
                    By.CSS_SELECTOR, LOAD_MORE_BUTTON_CSS_SELECTOR)
                load_more_button.click()
                time.sleep(1)
            except ElementClickInterceptedException:
                try:
                    time.sleep(1)
                    dismiss_popup_button = self.bot.find_element(
                        By.CSS_SELECTOR, SURVEY_POPUP_CSS_SELECTOR)
                    dismiss_popup_button.click()
                    time.sleep(1)
                except Exception as e:
                    print(e)
            except ElementNotInteractableException:
                print('Entire Smiles page loaded')
                break

        results = {}
        for name, cssSelector in REWARD_DETAILS_CSS_SELECTORS.items():
            try:
                results[name] = self.bot.find_elements(
                    By.CSS_SELECTOR, cssSelector)
            except Exception as e:
                print(e)

        results_sorted = []
        logo_idx = 0

        for idx, _ in enumerate(results['Company Name']):
            tmp = reward.Reward()
            tmp.backgroundImage = results['Background Image'][idx].get_attribute(
                'src')
            tmp.offerType = results['Offer Type'][idx].text.strip()
            if tmp.offerType == 'ETISALAT BUNDLE':
                tmp.logo = REWARD_ORIGIN_LOGO
            else:
                tmp.logo = results['Logo'][logo_idx].get_attribute('src')
                logo_idx += 1
            tmp.companyName = results['Company Name'][idx].text
            tmp.offer = results['Offer'][idx].text
            tmp.cost = results['Cost'][idx].text
            tmp.rewardOrigin = REWARD_ORIGIN
            tmp.rewardOriginLogo = REWARD_ORIGIN_LOGO
            results_sorted.append(tmp)
        return results_sorted
