import requests
from bs4 import BeautifulSoup
import re
from scraping_scripts.reward_object import Reward
import logging
import hashlib
import scraping_scripts.gmaps_location_helper as gmaps

URL = 'https://www.ubyemaar.com'
CATEGORY_TYPES = {
    'dine': 'Dining',
    'stay': 'Travel',
    'play': 'Entertainment',
    'relax': 'Health'
}

OFFER_LINKS_CSS_SELECTOR = '#offer-container .thumb a'

REWARD_ORIGIN = 'U by Emaar'
REWARD_ORIGIN_LOGO = 'http://www.dubaichronicle.com/wp-content/uploads/2016/02/U-Emaar-Logo_tcm130-92570-696x348.jpg'
SLUG = 'u_by_emaar'

REWARD_DETAILS_CSS_SELECTORS = {
    'Background Image': '.slider picture img',
    'Company Name': '.thumb_detail_content h3',
    'Offer': '.thumbBox .hdng-sty5',
    'Cost': '.sec_experience-detail .row .row .col-md-6:nth-child(3) .widget-content',
    'Offer Details': '.about-content',
    'Working Hours': '.sec_experience-detail .row .row .col-md-6:first-child .widget-content',
    'Contact and Website': '.sec_experience-detail .row .row .col-md-6:nth-child(4) .widget-content a.widget-link',
    'Expiry Date': '.thumbBox .exp_inline',
    'Terms and Conditions': '.card-body',
    'Address' : '.experienceList .thumb_detail_content a:nth-of-type(1) p:nth-of-type(2)'
}

IMAGE_REGEX_EXPRESSION = r'(http.*(\bjpg\b|\bpng\b|\bjpeg\b|\bJPG\b|\bPNG\b|\bJPEG\b))'

class UByEmaar:
    def __init__(self):
        self.results = self.run_script()
        logging.info('{} successfully retrieved'.format(self.results[0].rewardOrigin))

    def run_script(self):
        r = requests.get(URL)
        data = r.text
        soup = BeautifulSoup(data, 'lxml')

        results = []
        for key, offer_type in CATEGORY_TYPES.items():
            data = requests.get('{}/en/offers/{}'.format(URL, key)).text
            soup = BeautifulSoup(data, 'lxml')
            offerLinks = [i.get('href') for i in soup.select(OFFER_LINKS_CSS_SELECTOR)]
            for link in offerLinks:
                tmp = Reward()
                tmp.link = '{}{}'.format(URL, link)
                logging.info('Checking out link {} of {}'.format(tmp.link,SLUG))
                data = requests.get(tmp.link).text
                soup = BeautifulSoup(data, 'lxml')
                tmp.offerType = offer_type
                offer = soup.select(REWARD_DETAILS_CSS_SELECTORS['Offer'])[0].text
                offer = offer.replace(' At ', ' at ')
                if ' at ' in offer:
                    tmp.offer = offer.split(' at ')[0].strip()
                else:
                    tmp.offer = offer.strip()
                company_name = soup.select(
                    REWARD_DETAILS_CSS_SELECTORS['Company Name'])
                if not company_name:
                    tmp.companyName = offer.split(' at ')[1].strip()
                else:
                    tmp.companyName = company_name[0].text.strip()
                image_link_partial = soup.select(
                    REWARD_DETAILS_CSS_SELECTORS['Background Image'])[0].get('src')
                image_link_full = '{}{}'.format(URL, image_link_partial)
                tmp.backgroundImage = re.search(
                    IMAGE_REGEX_EXPRESSION, image_link_full).group(0)
                working_hours_unformatted = soup.select(
                    REWARD_DETAILS_CSS_SELECTORS['Working Hours'])[0]
                if isinstance(working_hours_unformatted, list):
                    tmp.workingHours = working_hours_unformatted.contents[-1].strip(
                    ) if working_hours_unformatted.contents[-1] != '\n' else working_hours_unformatted.contents[-2].strip()
                else:
                    tmp.workingHours = working_hours_unformatted.text.strip()
                tmp.workingHours = tmp.workingHours.replace(
                    'Opening Hours', '').strip()
                cost_unformatted = soup.select(REWARD_DETAILS_CSS_SELECTORS['Cost'])[0]
                if isinstance(cost_unformatted, list):
                    tmp.cost = cost_unformatted.contents[-1].strip(
                    ) if cost_unformatted.contents[-1] != '\n' else cost_unformatted.contents[-2].strip()
                else:
                    tmp.cost = cost_unformatted.text.strip()
                tmp.cost = tmp.cost.replace('Price', '').strip()
                tmp.offerDescription = soup.select(REWARD_DETAILS_CSS_SELECTORS['Offer Details'])[
                    0].text.replace('About This Experience', '').replace('View more details','').strip()
                tmp.expiryDate = soup.select(REWARD_DETAILS_CSS_SELECTORS['Expiry Date'])[
                    0].text.strip()
                tmp.termsAndConditions = soup.select(
                    REWARD_DETAILS_CSS_SELECTORS['Terms and Conditions'])[0].text.strip()
                contact_and_link = soup.select(
                    REWARD_DETAILS_CSS_SELECTORS['Contact and Website'])
                tmp.website = contact_and_link[0].get('href')
                tmp.contact = contact_and_link[1].text.strip()
                tmp.rewardOrigin = REWARD_ORIGIN
                tmp.rewardOriginLogo = REWARD_ORIGIN_LOGO
                address = [i.text.strip() for i in soup.select(REWARD_DETAILS_CSS_SELECTORS['Address'])]
                hashid = '{}{}{}{}'.format(
                SLUG, address, tmp.companyName,tmp.offer).encode('utf-8')
                tmp.id = str(int(hashlib.md5(hashid).hexdigest(), 16))
                for add in address:
                    if '|' in add:
                        addresses = add.split('|')
                        for adds in addresses:
                            gmaps.getLocationId(tmp.id, adds)
                    else:
                        gmaps.getLocationId(tmp.id, add)
                tmp.slug = SLUG
                results.append(tmp)
        return results