from bs4 import BeautifulSoup
import requests
import logging
import scraping_scripts.reward_object as reward

URL = 'https://www.aus.edu/discount-program'
REWARD_ORIGIN = 'AUS Discount Program'
REWARD_ORIGIN_LOGO = 'https://ih0.redbubble.net/image.402393616.9206/flat,1000x1000,075,f.jpg'
SLUG = 'aus_discount_program'

DISCOUNT_CATEGORIES_CSS_SELECTOR = '#edit-field-discount-category-tid-wrapper a'
DISCOUNT_LIST_CSS_SELECTOR = 'views-row'
REWARD_DETAILS_CSS_SELECTORS = {
    'Background Image': 'views-field-field-banner-image',
    'Company Name': 'views-field-title',
    'Offer': 'views-field-body',
    'Location': 'views-field-field-discount-location',
    'Contact': 'views-field-field-discount-phone-number',
}
CATEGORY_TYPES = {
    'Automotive' : 'Services',
    'Dining' : 'Dining',
    'Education' : 'Services',
    'Hotels/Resorts' : 'Travel',
    'Leisure/Adventure' : 'Leisure',
    'Medical/Dental/Nutrition' : 'Health',
    'Opticians' : 'Health',
    'Retail' : 'Retail',
    'Services' : 'Services',
    'Wellness/Beauty' : 'Health'
}
class AusDiscountProgram:
    def __init__(self):
        self.results = self.run_script()
        logging.info('{} successfully retrieved'.format(self.results[0].rewardOrigin))

    def run_script(self):
        r = requests.get(url= URL)
        data = r.text
        soup = BeautifulSoup(data, 'lxml')
        results = []
        categories = soup.select(DISCOUNT_CATEGORIES_CSS_SELECTOR)[1:]
        for category in categories:
            categoryDiscounts = requests.get(url= category['href'])
            data = categoryDiscounts.text
            soup = BeautifulSoup(data, 'lxml')
            discounts = soup.find_all('div', attrs = {'class' : DISCOUNT_LIST_CSS_SELECTOR})
            for discount in discounts:
                tmp = reward.Reward()
                tmp.backgroundImage = discount.find('div', attrs = {'class': REWARD_DETAILS_CSS_SELECTORS['Background Image']}).find('img')['src']
                tmp.companyName = discount.find('div', attrs = {'class': REWARD_DETAILS_CSS_SELECTORS['Company Name']}).text.strip()
                if len(discount.find_all('li')) > 1:
                    tmp.offer = 'Multiple Offers'
                    tmp.offerDescription = discount.find('div', attrs={'class':REWARD_DETAILS_CSS_SELECTORS['Offer']}).text.strip()
                else:
                    tmp.offer = discount.find('div', attrs = {'class': REWARD_DETAILS_CSS_SELECTORS['Offer']}).text.strip()
                    tmp.offerDescription = ''
                tmp.location = discount.find('div', attrs = {'class': REWARD_DETAILS_CSS_SELECTORS['Location']}).text.strip()
                contact = discount.find('div', attrs = {'class': REWARD_DETAILS_CSS_SELECTORS['Contact']}).text.strip()
                if contact:
                    tmp.contact = contact
                else:
                    tmp.contact = ''
                tmp.offerType =CATEGORY_TYPES[category.text.strip()]
                tmp.rewardOrigin = REWARD_ORIGIN
                tmp.rewardOriginLogo = REWARD_ORIGIN_LOGO
                tmp.slug = SLUG
                results.append(tmp)
        return results