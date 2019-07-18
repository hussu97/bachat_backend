from bs4 import BeautifulSoup
import requests
import sqlite
class Reward:
    pass

URL = 'https://www.aus.edu/discount-program'

REWARD_ORIGIN = 'AUS Discount Program'
REWARD_ORIGIN_LOGO = 'https://ih0.redbubble.net/image.402393616.9206/flat,1000x1000,075,f.jpg'

DISCOUNT_CATEGORIES_CSS_SELECTOR = '#edit-field-discount-category-tid-wrapper a'
DISCOUNT_LIST_CSS_SELECTOR = 'views-row'
REWARD_DETAILS_CSS_SELECTORS = {
    'Background Image': 'views-field-field-banner-image',
    'Company Name': 'views-field-title',
    'Offer': 'views-field-body',
    'Location' : 'views-field-field-discount-location',
    'Contact' : 'views-field-field-discount-phone-number',

}

r = requests.get(url = URL)
data = r.text
soup = BeautifulSoup(data, 'lxml')
results = []

categories = soup.select(DISCOUNT_CATEGORIES_CSS_SELECTOR)[1:]
for category in categories:
    categoryDiscounts = requests.get(url = category['href'])
    data = categoryDiscounts.text
    soup = BeautifulSoup(data, 'lxml')
    discounts = soup.find_all('div',attrs = {'class' : DISCOUNT_LIST_CSS_SELECTOR})
    for discount in discounts:
        tmp = Reward()
        tmp.backgroundImage = discount.find('div', attrs = {'class' : REWARD_DETAILS_CSS_SELECTORS['Background Image']}).find('img')['src']
        tmp.companyName = discount.find('div', attrs = {'class' : REWARD_DETAILS_CSS_SELECTORS['Company Name']}).text.strip()
        if len(discount.find_all('li')) > 1 :
            tmp.offer = 'Multiple Offers'
            tmp.offerDescription = discount.find('div',attrs={'class':REWARD_DETAILS_CSS_SELECTORS['Offer']}).text.strip()
        else :
            tmp.offer = discount.find('div', attrs = {'class' : REWARD_DETAILS_CSS_SELECTORS['Offer']}).text.strip()
        tmp.location = discount.find('div', attrs = {'class' : REWARD_DETAILS_CSS_SELECTORS['Location']}).text.strip()
        contact = discount.find('div', attrs = {'class' : REWARD_DETAILS_CSS_SELECTORS['Contact']}).text.strip()
        if contact:
            tmp.contact = contact
        tmp.offerType = category.text.strip()
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
    location,
    offer_description,
    contact
) values (
    ?,?,?,?,?,?,?,?,?
)"""
INSERT_STATEMENT_2 = """INSERT INTO rewards(
    reward_origin, 
    reward_origin_logo, 
    background_image,
    offer, 
    offer_type, 
    company_name,
    location,
    contact
) values (
    ?,?,?,?,?,?,?,?
)"""
INSERT_STATEMENT_3 = """INSERT INTO rewards(
    reward_origin, 
    reward_origin_logo, 
    background_image,
    offer, 
    offer_type, 
    company_name,
    location,
    offer_description
) values (
    ?,?,?,?,?,?,?,?
)"""
INSERT_STATEMENT_4 = """INSERT INTO rewards(
    reward_origin, 
    reward_origin_logo, 
    background_image,
    offer, 
    offer_type, 
    company_name,
    location
) values (
    ?,?,?,?,?,?,?
)"""
for i in results:
    if hasattr(i,'contact') and hasattr(i, 'offerDescription'):
        sqlConn.insert_reward(
            INSERT_STATEMENT_1,
            (
                REWARD_ORIGIN,
                REWARD_ORIGIN_LOGO,
                i.backgroundImage,
                i.offer,
                i.offerType,
                i.companyName,
                i.location,
                i.offerDescription,
                i.contact
            )
        )
    elif hasattr(i,'contact'):
        sqlConn.insert_reward(
            INSERT_STATEMENT_2,
            (
                REWARD_ORIGIN,
                REWARD_ORIGIN_LOGO,
                i.backgroundImage,
                i.offer,
                i.offerType,
                i.companyName,
                i.location,
                i.contact
            )
        )
    elif hasattr(i, 'offerDescription'):
        sqlConn.insert_reward(
            INSERT_STATEMENT_3,
            (
                REWARD_ORIGIN,
                REWARD_ORIGIN_LOGO,
                i.backgroundImage,
                i.offer,
                i.offerType,
                i.companyName,
                i.location,
                i.offerDescription
            )
        )
    else :
        sqlConn.insert_reward(
            INSERT_STATEMENT_4,
            (
                REWARD_ORIGIN,
                REWARD_ORIGIN_LOGO,
                i.backgroundImage,
                i.offer,
                i.offerType,
                i.companyName,
                i.location
            )
        )