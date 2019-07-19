from bs4 import BeautifulSoup
import requests

url = 'https://www.theentertainerme.com/search-outlets/index?page=6&SearchOutletsForm%5Bquery%5D=&SearchOutletsForm%5Bproduct_id%5D=4421&SearchOutletsForm%5Bcuisine%5D=&SearchOutletsForm%5Bcountry%5D=&SearchOutletsForm%5Bstarratings%5D=&offers=on'

r = requests.get(url)
data = r.text
soup = BeautifulSoup(data, 'lxml')
print(soup.find_all('div', attrs = {'id' : 'results'}))