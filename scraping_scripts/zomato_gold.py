from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException
import time
import re
import scraping_scripts.reward_object as reward
import scraping_scripts.gmaps_location_helper as gmaps
import logging
import hashlib

URL_1 = 'https://www.zomato.com/'
URL_2 = '/restaurants?gold_partner=1'
COUNTRY_LIST = ['sharjah', 'abudhabi', 'fujairah',  'dubai',  'ajman',
                'umm-al-quwain', 'al-ain',  'ras-al-khaimah']

REWARD_DETAILS_CSS_SELECTORS = {
    'Background Image': '.search_left_featured a',
    'Company Name': '.result-title',
    'Offer': '.red_res_tag img',
    'Offer Type': '.res-snippet-small-establishment',
    'Cost': '.res-cost .col-s-11',
    'Cuisine': '.search-page-text div:first-child .col-s-11',
    'Location': '.search_result_subzone',
    'Working Hours': '.res-timing',
    'Contact': '.res-snippet-ph-info',
    'Link': '.result-title',
    'Rating': '.rating-popup'
}

IMAGE_REGEX_EXPRESSION = r'(http.*(\bjpg\b|\bpng\b|\bjpeg\b|\bJPG\b|\bPNG\b|\bJPEG\b))'

REWARD_ORIGIN = 'Zomato Gold'
REWARD_ORIGIN_LOGO = 'https://upload.wikimedia.org/wikipedia/commons/e/ef/Zomato-flat-logo.png'

NO_REWARDS_CSS_SELECTOR = '.search-empty-message-container'

PAGE_NUMBER_CSS_SELECTOR = '.pagination-number'
NEXT_PAGE_BUTTON_CSS_SELECTOR = '.pagination-control .next'

FOOD_OFFER_IMG = 'https://www.zomato.com//images/red/gold_blue_tag_1_en.png'
DRINKS_OFFER_IMG = 'https://www.zomato.com//images/red/gold_blue_tag_2_en.png'


class ZomatoGold:
    def __init__(self, country):
        self.country = country
        self.slug = 'zomato_gold_{}'.format(self.country)
        options = Options()
        options.headless = True
        self.bot = webdriver.Firefox(options=options)
        self.results = self.run_script()
        logging.info('{} successfully retrieved'.format(self.slug))
        self.bot.quit()

    def run_script(self):
        results = []
        self.bot.get(URL_1+self.country+URL_2)
        time.sleep(1)
        try:
            self.bot.find_element(By.CSS_SELECTOR, NO_REWARDS_CSS_SELECTOR)
            logging.info('no gold partners for {} in Zomato'.format(self.slug))
        except NoSuchElementException:
            pageResults = self.execute_script(1, self.country)
            if pageResults is not None:
                results = results + pageResults
            numberOfPages = int(self.bot.find_element(
                By.CSS_SELECTOR, PAGE_NUMBER_CSS_SELECTOR).text.split(' ')[3])

            for j in range(numberOfPages-1):
                nextPage = self.bot.find_element(
                    By.CSS_SELECTOR, NEXT_PAGE_BUTTON_CSS_SELECTOR)
                nextPage.click()
                time.sleep(1)
                pageResults = self.execute_script(j+2, self.country)
                if pageResults is not None:
                    results = results + pageResults
            logging.info('{} successfully updated'.format(self.slug))
        return results

    def execute_script(self, pageNum, city):
        page_css_elements = {}
        # Get scroll height
        document_height = self.bot.execute_script(
            "return document.body.scrollHeight")
        for i in range(0, document_height, 1080):
            # Scroll down to self.bottom
            self.bot.execute_script("window.scrollTo(0, {});".format(i))
            # Wait to load page
            time.sleep(1)
        time.sleep(1)
        for name, cssSelector in REWARD_DETAILS_CSS_SELECTORS.items():
            try:
                page_css_elements[name] = (
                    self.bot.find_elements(By.CSS_SELECTOR, cssSelector))
            except Exception as e:
                logging.warning('Elements not found in {} of {}. Error msg {}'.format(
                    pageNum, self.slug, e))
        offerTypeIdx = 0
        results = []
        for idx, _ in enumerate(page_css_elements['Company Name']):
            tmp = reward.Reward()
            try:
                tmp.backgroundImage = re.search(
                    IMAGE_REGEX_EXPRESSION, page_css_elements['Background Image'][idx].get_attribute('style')).group(0)
            except Exception as e:
                logging.error('found {} error in page {} item {} for {} place'.format(
                    e, pageNum, idx+1, city))
            tmp.companyName = page_css_elements['Company Name'][idx].text.strip(
            )
            tmp.offer = '1+1 on Food' if page_css_elements['Offer'][idx].get_attribute(
                'src') == FOOD_OFFER_IMG else '2+2 on Drinks'
            if tmp.companyName == 'Castle Restaurant':
                tmp.offerType = 'CASUAL DINING'
                logging.info('found the anomaly')
            else:
                tmp.offerType = page_css_elements['Offer Type'][offerTypeIdx].text.strip(
                )
                offerTypeIdx += 1
            tmp.cost = page_css_elements['Cost'][idx].text.strip() + ' for Two'
            tmp.cuisine = page_css_elements['Cuisine'][idx].text.strip()
            address = tmp.companyName + ' ' + \
                page_css_elements['Location'][idx].text.strip()
            tmp.workingHours = page_css_elements['Working Hours'][idx].get_attribute(
                'title').strip()
            tmp.contact = page_css_elements['Contact'][idx].get_attribute(
                'data-phone-no-str').strip()
            tmp.link = page_css_elements['Link'][idx].get_attribute('href')
            tmp.rating = page_css_elements['Rating'][idx].text.strip()
            tmp.rewardOrigin = REWARD_ORIGIN
            tmp.rewardOriginLogo = REWARD_ORIGIN_LOGO
            hashid = '{}{}{}'.format(
                self.slug, address, tmp.companyName).encode('utf-8')
            tmp.id = str(int(hashlib.md5(hashid).hexdigest(), 16))
            gmaps.getLocationId(tmp.id, address)
            tmp.slug = self.slug
            results.append(tmp)
        return results
