from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import ElementClickInterceptedException, ElementNotInteractableException, NoSuchElementException
import logging
import time
import hashlib
from scraping_scripts.reward_object import Reward
import scraping_scripts.gmaps_location_helper as gmaps

URL = 'https://smiles.etisalat.ae/Smiles-Portal-1.0.0/#/partners'
# URL = 'https://smiles.etisalat.ae'

REWARD_ORIGIN = 'Etisalat Smiles'
SLUG = 'etisalat_smiles'
REWARD_ORIGIN_LOGO = 'https://seeklogo.com/images/E/Etisalat-logo-A58AC10542-seeklogo.com.png'

VIEW_ALL_PAGE_LOADED_CSS_SELECTOR = '.deal-teaser__image'
VIEW_ALL_REWARD_PAGE_LOADED_CSS_SELECTOR = '.table-social'
SURVEY_POPUP_CSS_SELECTOR = '.survey-popup-buttons button'
REWARD_DETAILS_CSS_SELECTORS = {
    'Background Image': '.deal-teaser__image img',
    'Logo': '.text-and-table .cover img',
    'Company Name': '.table-title h5',
    'Offer': '.deal-info__description',
    'Offer Description': '.text-and-table-table .row:nth-of-type(2) .col-sm-8',
    'Offer Type': '#menu-item-{}',
    'Cost': '.deal-price__value',
    'Expiry Date': '.text-and-table-table .row:nth-of-type(5) .col-sm-8',
    'Link': '.table-social-icons .social-icon a:nth-of-type(1)',
    'Terms And Conditions': '#termsandconditions',
    'Address': '.mapItem'
}
CATEGORY_TYPES = {
    'SHOPPING': 'Retail',
    'DINING': 'Dining',
    'ENTERTAINMENT': 'Entertainment',
    'WELLNESS': 'Health',
    'TRAVEL': 'Travel'
}
LINKS_LIST_CSS_SELECTOR = '#menu-content-{} .smiles-tab-content .smiles-tab-content-item a'
OFFERS_LIST_CSS_SELECTOR = '.similar-grid-boxes .grid-item'
OFFER_FILTER_CSS_SELECTOR = '.deal-info__tag span'
CLICKABLE_OFFER = 'DISCOUNT'
CLICK_OFFER_BUTTON_CSS_SELECTOR = '.deal-info'
SHOW_MORE_CSS_SELECTOR = '.text-and-table-table .row:nth-of-type(2) .col-sm-8 .ng-scope'
NUM_LOCATION_PAGES_CSS_SELECTOR = '.pagination p'
NEXT_LOCATION_PG_BUTTON_CSS_SELECTOR = '.next'


class EtisalatSmiles:
    def __init__(self):
        options = Options()
        options.headless = True
        self.bot = webdriver.Firefox(options=options)
        self.results = self.run_script()
        logging.info('{} successfully retrieved'.format(
            self.results[0].rewardOrigin))
        self.bot.quit()

    def run_script(self):
        self.bot.get(URL)
        time.sleep(2)
        try:
            dismiss_popup_button = self.bot.find_element(
                By.CSS_SELECTOR, SURVEY_POPUP_CSS_SELECTOR)
            dismiss_popup_button.click()
            time.sleep(1)
        except:
            logging.info('no popup found')
        linksWithCategories = {}
        for i in range(1, 6):
            category = CATEGORY_TYPES[self.bot.find_element(
                By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Offer Type'].format(i)).text]
            linksWithCategories[category] = [j.get_attribute('href') for j in self.bot.find_elements(
                By.CSS_SELECTOR, LINKS_LIST_CSS_SELECTOR.format(i))]
        results = []
        for key, values in linksWithCategories.items():
            for rewardLink in values:
                self.bot.get(rewardLink)
                try:
                    WebDriverWait(self.bot, 5).until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, VIEW_ALL_PAGE_LOADED_CSS_SELECTOR)))
                except:
                    continue
                try:
                    dismiss_popup_button = self.bot.find_element(
                        By.CSS_SELECTOR, SURVEY_POPUP_CSS_SELECTOR)
                    dismiss_popup_button.click()
                    time.sleep(1)
                except:
                    logging.info('no popup found')
                logging.info(
                    'Checking out link {} of {}'.format(rewardLink, SLUG))
                numLinks = self.bot.find_elements(
                    By.CSS_SELECTOR, OFFERS_LIST_CSS_SELECTOR)
                offerType = key
                logo = self.bot.find_element(
                    By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Logo']).get_attribute('src')
                companyName = self.bot.find_element(
                    By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Company Name']).text.strip()
                termsAndConditions = self.bot.find_element(
                    By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Terms And Conditions']).text.strip()
                locationPages = int(self.bot.find_element(
                    By.CSS_SELECTOR, NUM_LOCATION_PAGES_CSS_SELECTOR).text.strip()[-1])
                addresses = []
                time.sleep(1)
                while locationPages > 0:
                    addresses += [companyName + ' ' + a.find_element(By.CSS_SELECTOR, 'h3').text.strip() + ' ' + a.find_element(By.CSS_SELECTOR, 'p:nth-of-type(1)').text.strip(
                    ) for a in self.bot.find_elements(By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Address'])]
                    self.bot.find_element(
                        By.CSS_SELECTOR, NEXT_LOCATION_PG_BUTTON_CSS_SELECTOR).click()
                    locationPages -= 1
                for i in range(len(numLinks)):
                    tmp = Reward()
                    offerElements = self.bot.find_element(
                        By.CSS_SELECTOR, '{}:nth-of-type({})'.format(OFFERS_LIST_CSS_SELECTOR, i+1))
                    tmp.offer = offerElements.find_element(
                        By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Offer']).text.strip()
                    tmp.cost = offerElements.find_element(
                        By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Cost']).text.strip()
                    tmp.backgroundImage = offerElements.find_element(
                        By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Background Image']).get_attribute('src')
                    offerFilter = offerElements.find_element(
                        By.CSS_SELECTOR, OFFER_FILTER_CSS_SELECTOR).text.strip()
                    logging.info(offerFilter)
                    if offerFilter == CLICKABLE_OFFER:
                        logging.info('in')
                        offerElements.find_element(
                            By.CSS_SELECTOR, CLICK_OFFER_BUTTON_CSS_SELECTOR).click()
                        time.sleep(1)
                        WebDriverWait(self.bot, 10).until(EC.presence_of_element_located(
                            (By.CSS_SELECTOR, VIEW_ALL_REWARD_PAGE_LOADED_CSS_SELECTOR)))
                        tmp.expiryDate = self.bot.find_element(
                            By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Expiry Date']).text.strip()
                        tmp.offerDescription = self.bot.find_element(
                            By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Offer Description']).text.strip()
                        tmp.link = self.bot.find_element(
                            By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Link']).get_attribute('socialshare-url')
                        self.bot.execute_script("window.history.go(-1)")
                        time.sleep(1)
                    tmp.offerType = offerType
                    tmp.logo = logo
                    tmp.companyName = companyName
                    tmp.termsAndConditions = termsAndConditions
                    hashid = '{}{}{}'.format(
                        SLUG, tmp.offer, tmp.companyName).encode('utf-8')
                    tmp.id = str(int(hashlib.md5(hashid).hexdigest(), 16))
                    for z in addresses:
                        gmaps.getLocationId(tmp.id, z)
                    tmp.rewardOrigin = REWARD_ORIGIN
                    tmp.rewardOriginLogo = REWARD_ORIGIN_LOGO
                    tmp.slug = SLUG
                    results.append(tmp)
        return results
