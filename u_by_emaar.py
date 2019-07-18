import requests
from bs4 import BeautifulSoup
import re
import sqlite


class Reward:
    pass


URL = 'https://www.ubyemaar.com'
CATEGORY_TYPES = {
    'dine': 'Dining',
    'stay': 'Hotels',
    'play': 'Entertainment',
    'relax': 'Health'
}

OFFER_LINKS_CSS_SELECTOR = '#offer-container .thumb a'

REWARD_ORIGIN = 'U by Emaar'
REWARD_ORIGIN_LOGO = 'http://www.dubaichronicle.com/wp-content/uploads/2016/02/U-Emaar-Logo_tcm130-92570-696x348.jpg'

REWARD_DETAILS_CSS_SELECTORS = {
    'Background Image': '.slider picture img',
    'Company Name': '.thumb_detail_content h3',
    'Offer': '.thumbBox .hdng-sty5',
    'Cost': '.sec_experience-detail .row .row .col-md-6:nth-child(3) .widget-content',
    'Offer Details': '.about-content',
    'Location': '.search-result-address',
    'Working Hours': '.sec_experience-detail .row .row .col-md-6:first-child .widget-content',
    'Contact and Link': '.sec_experience-detail .row .row .col-md-6:nth-child(4) .widget-content a.widget-link',
    'Expiry Date': '.thumbBox .exp_inline',
    'Terms and Conditions': '.card-body'
}

IMAGE_REGEX_EXPRESSION = r'(http.*(\bjpg\b|\bpng\b|\bjpeg\b|\bJPG\b|\bPNG\b|\bJPEG\b))'

r = requests.get(URL)
data = r.text
soup = BeautifulSoup(data, 'lxml')

results = []
for key, offer_type in CATEGORY_TYPES.items():
    data = requests.get('{}/en/offers/{}'.format(URL, key)).text
    soup = BeautifulSoup(data, 'lxml')
    print('hi')
    offerLinks = [i.get('href') for i in soup.select(OFFER_LINKS_CSS_SELECTOR)]
    for link in offerLinks:
        tmp = Reward()
        data = requests.get('{}{}'.format(URL, link)).text
        soup = BeautifulSoup(data, 'lxml')
        print('hi again')
        tmp.offer_type = offer_type
        offer = soup.select(REWARD_DETAILS_CSS_SELECTORS['Offer'])[0].text
        offer = offer.replace(' At ', ' at ')
        if ' at ' in offer:
            tmp.offer = offer.split(' at ')[0].strip()
        else:
            tmp.offer = offer.strip()
        company_name = soup.select(
            REWARD_DETAILS_CSS_SELECTORS['Company Name'])
        if not company_name:
            tmp.company_name = offer.split(' at ')[1].strip()
        else:
            tmp.company_name = company_name[0].text.strip()
        image_link_partial = soup.select(
            REWARD_DETAILS_CSS_SELECTORS['Background Image'])[0].get('src')
        image_link_full = '{}{}'.format(URL, image_link_partial)
        tmp.background_image = re.search(
            IMAGE_REGEX_EXPRESSION, image_link_full).group(0)
        working_hours_unformatted = soup.select(
            REWARD_DETAILS_CSS_SELECTORS['Working Hours'])[0]
        if isinstance(working_hours_unformatted, list):
            tmp.working_hours = working_hours_unformatted.contents[-1].strip(
            ) if working_hours_unformatted.contents[-1] != '\n' else working_hours_unformatted.contents[-2].strip()
        else:
            tmp.working_hours = working_hours_unformatted.text.strip()
        tmp.working_hours = tmp.working_hours.replace(
            'Opening Hours', '').strip()
        cost_unformatted = soup.select(REWARD_DETAILS_CSS_SELECTORS['Cost'])[0]
        if isinstance(cost_unformatted, list):
            tmp.cost = cost_unformatted.contents[-1].strip(
            ) if cost_unformatted.contents[-1] != '\n' else cost_unformatted.contents[-2].strip()
        else:
            tmp.cost = cost_unformatted.text.strip()
        tmp.cost = tmp.cost.replace('Price', '').strip()
        tmp.offer_description = soup.select(REWARD_DETAILS_CSS_SELECTORS['Offer Details'])[
            0].text.replace('About This Experience', '').replace('View more details','').strip()
        tmp.expiry_date = soup.select(REWARD_DETAILS_CSS_SELECTORS['Expiry Date'])[
            0].text.strip()
        tmp.terms_and_conditions = soup.select(
            REWARD_DETAILS_CSS_SELECTORS['Terms and Conditions'])[0].text.strip()
        contact_and_link = soup.select(
            REWARD_DETAILS_CSS_SELECTORS['Contact and Link'])
        tmp.link = contact_and_link[0].get('href')
        tmp.contact = contact_and_link[1].text.strip()
        results.append(tmp)

sqlConn = sqlite.SQLConnector()

if len(sqlConn.select_by_reward_origin(REWARD_ORIGIN)) > 0:
    sqlConn.delete_by_reward_origin(REWARD_ORIGIN)

INSERT_STATEMENT_1 = """INSERT INTO rewards(
    reward_origin, 
    reward_origin_logo, 
    background_image,
    offer, 
    offer_type, 
    company_name,
    offer_description,
    contact,
    working_hours,
    expiry_date,
    link,
    terms_and_conditions,
    cost
) values (
    ?,?,?,?,?,?,?,?,?,?,?,?,?
)"""

for i in results:
    sqlConn.insert_reward(
        INSERT_STATEMENT_1,
        (
            REWARD_ORIGIN,
            REWARD_ORIGIN_LOGO,
            i.background_image,
            i.offer,
            i.offer_type,
            i.company_name,
            i.offer_description,
            i.contact,
            i.working_hours,
            i.expiry_date,
            i.link,
            i.terms_and_conditions,
            i.cost
        )
    )
