from scraping_scripts.entertainer import Entertainer


# URL = 'https://www.theentertainerme.com/search-outlets/index?SearchOutletsForm%5Blocation_id%5D=1&SearchOutletsForm%5Bquery%5D=&SearchOutletsForm%5Bproduct_id%5D=4421&SearchOutletsForm%5Bcuisine%5D=&SearchOutletsForm%5Bcountry%5D=&SearchOutletsForm%5Bstarratings%5D=&offers=on&page=1'
# URL = 'https://www.theentertainerme.com/search-outlets/index?SearchOutletsForm%5Bquery%5D=&SearchOutletsForm%5Bproduct_id%5D=5964&SearchOutletsForm%5Bcuisine%5D=&SearchOutletsForm%5Bcountry%5D=&SearchOutletsForm%5Bstarratings%5D=&offers=on&page=1'
URL = 'https://www.theentertainerme.com/outlets?SearchOutletsForm[location_id]=1&SearchOutletsForm[product_id]={}'
ABU_DHABI_BODY = 5964
ABU_DHABI = 4995
DUBAI = 4421
class EntertainerDubai:
    def __init__(self):
        self.results = Entertainer(URL.format(ABU_DHABI_BODY)).results