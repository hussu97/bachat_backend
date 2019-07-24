from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, NoAlertPresentException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import time
import re
import hashlib
from scraping_scripts.reward_object import Reward
import scraping_scripts.gmaps_location_helper as gmaps

REWARD_DETAILS_CSS_SELECTORS = {
    'Background Image': '.main-gallery .slideset .active img',
    'Company Name': '.content-desc h1',
    'Logo': '.hotel-logo img',
    'Rewards Origin and Details': '.product-sec .product-block',
    'Reward Origin': '.product-col .description h2',
    'Offer And Type And Expiry Date': '.offer-tab',
    'Offer': 'h3',
    'Expiry Date': '.list-inline li:nth-of-type(2)',
    'Offer Type': '.icon-holder img',
    'Details': '.info-list',
    'Rating': '.review-links img',
}
CATEGORY_TYPES = {
    'Leisure': 'Entertainment',
    'Travel': 'Travel',
    'Restaurants and Bars': 'Dining',
    'Services': 'Services',
    'Retail': 'Retail',
    'Body': 'Health'
}

REWARD_ORIGIN_LOGO = 'https://etsitecdn.theentertainerme.com/logo.png'

NEXT_PAGE_BUTTON_CSS_SELECTOR = '#paginate_container .next'
RESULTS_LIST_CSS_SELECTOR = '#results .list_view .merchant a'

RESET_BROWSER_DATA_PAGES = 15


class Entertainer:
    def __init__(self, url, slug):
        self.url = url
        self.slug = slug
        options = Options()
        options.headless = True
        self.bot = webdriver.Firefox(options=options)
        self.results = self.run_script()
        logging.info('{} successfully retrieved'.format(
            self.results[0].rewardOrigin))
        self.bot.close()
        self.bot.quit()

    def run_script(self):
        results = []
        rewards_links = []
        self.bot.get(self.url)
        while True:
            try:
                self.bot.switch_to_alert().dismiss()
                logging.info('alert found while collecting list links')
                time.sleep(5)
                self.bot.switch_to_alert().dismiss()
                logging.info('removed alert again')
            except NoAlertPresentException as e:
                pass
            rewards_links = rewards_links + [i.get_attribute('href') for i in self.bot.find_elements(
                By.CSS_SELECTOR, RESULTS_LIST_CSS_SELECTOR)]
            logging.info(
                'Reward links for {} retrieved so far - {}'.format(self.slug, len(rewards_links)))
            try:
                self.bot.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                next_page_button = self.bot.find_element(
                    By.CSS_SELECTOR, NEXT_PAGE_BUTTON_CSS_SELECTOR)
                next_page_button.click()
            except ElementClickInterceptedException:
                logging.info('Reached end of {}'.format(self.slug))
                break
        for idx, i in enumerate(rewards_links):
            if idx % RESET_BROWSER_DATA_PAGES == 0:
                self.bot.quit()
                options = Options()
                options.headless = True
                self.bot = webdriver.Firefox(options=options)
            try:
                self.bot.get(i)
            except TimeoutException as e:
                logging.info('{} did not load in {}'.format(i, self.slug))
                continue
            logging.info('Checking out link {} of {}'.format(i, self.slug))
            try:
                self.bot.switch_to_alert().dismiss()
                logging.info('alert found when parsing companies')
                WebDriverWait(self.bot, 20).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Background Image'])))
            except NoAlertPresentException as e:
                pass
            link = i
            backgroundImage = self.bot.find_element(
                By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Background Image']).get_attribute('src')
            companyName = self.bot.find_element(
                By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Company Name']).text.strip()
            logo = self.bot.find_element(
                By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Logo']).get_attribute('src')
            details = self.bot.find_element(
                By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Details']).text.strip()
            details = details.split('\n')
            area = ''
            website = ''
            hotel = ''
            location = ''
            mall = ''
            contact = ''
            cuisine = ''
            for k in range(len(details)-1, 0, -2):
                if details[k-1] == 'Website':
                    website = details[k]
                elif details[k-1] == 'Phone':
                    contact = details[k]
                elif details[k-1] == 'Area':
                    area = details[k]
                elif details[k-1] == 'Hotel':
                    hotel = details[k]
                elif details[k-1] == 'Location':
                    location = details[k]
                elif details[k-1] == 'Mall':
                    mall = details[k]
                elif details[k-1] == 'Email':
                    pass
                elif details[k-1] == 'Cuisine':
                    cuisine = details[k]
                else:
                    logging.info('{} - {}'.format(details[k-1], details[k]))
            try:
                ratingImage = self.bot.find_element(
                    By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Rating']).get_attribute('src')
                rating = ratingImage.split("/")[-1].split("-")[0]
            except NoSuchElementException:
                rating = ''
            rewardOriginAndDetails = self.bot.find_elements(
                By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Rewards Origin and Details'])
            for l in rewardOriginAndDetails:
                rewardOrigin = l.find_element(
                    By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Reward Origin']).text.strip()
                offerTypeExpiry = l.find_elements(
                    By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Offer And Type And Expiry Date'])
                for j in offerTypeExpiry:
                    tmp = Reward()
                    tmp.offer = j.find_element(
                        By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Offer']).text.strip()
                    offerType = j.find_element(
                        By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Offer Type']).get_attribute('alt')
                    try:
                        tmp.offerType = CATEGORY_TYPES[offerType]
                    except Exception as e:
                        logging.info(offerType)
                    tmp.expiryDate = j.find_element(
                        By.CSS_SELECTOR, REWARD_DETAILS_CSS_SELECTORS['Expiry Date']).text.replace("Valid until", "").strip()
                    tmp.link = link
                    tmp.backgroundImage = backgroundImage
                    tmp.companyName = companyName
                    tmp.logo = logo
                    tmp.website = website
                    tmp.contact = contact
                    address = ''
                    if mall != '' and hotel != '':
                        address = hotel + ' ' + mall
                    elif mall == '' and hotel != '':
                        address = hotel
                    elif hotel == '' and mall != '':
                        address = mall
                    else:
                        address = location
                    address += ' ' + area
                    hashid = '{}{}{}'.format(
                        self.slug, tmp.offer, tmp.companyName).encode('utf-8')
                    tmp.id = str(int(hashlib.md5(hashid).hexdigest(), 16))
                    gmaps.getLocationId(tmp.id, address)
                    tmp.cuisine = cuisine
                    tmp.rating = rating
                    tmp.rewardOrigin = rewardOrigin
                    tmp.rewardOriginLogo = REWARD_ORIGIN_LOGO
                    tmp.slug = self.slug
                    results.append(tmp)
        return results
