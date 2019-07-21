from scraping_scripts.entertainer import Entertainer

URL = 'https://www.theentertainerme.com/outlets?SearchOutletsForm[location_id]=1&SearchOutletsForm[product_id]={}'
ABU_DHABI_BODY = 5964
ABU_DHABI = 4995
DUBAI = 4421
class Dubai:
    def __init__(self):
        self.results = Entertainer(URL.format(DUBAI),'entertainer_dubai').results
class AbuDhabi:
    def __init__(self):
        self.results = Entertainer(URL.format(ABU_DHABI),'entertainer_abu_dhabi').results
class AbuDhabiBody:
    def __init__(self):
        self.results = Entertainer(URL.format(ABU_DHABI_BODY),'entertainer_abu_dhabi_body').results