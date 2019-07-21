from scraping_scripts.zomato_gold import ZomatoGold

COUNTRY_LIST = ['sharjah', 'abudhabi', 'fujairah',  'dubai',  'ajman',
                'umm-al-quwain', 'al-ain',  'ras-al-khaimah']

class Sharjah:
    def __init__(self):
        self.results = ZomatoGold('sharjah').results
class Dubai:
    def __init__(self):
        self.results = ZomatoGold('dubai').results
class Fujairah:
    def __init__(self):
        self.results = ZomatoGold('fujairah').results
class Abudhabi:
    def __init__(self):
        self.results = ZomatoGold('abudhabi').results
class Ajman:
    def __init__(self):
        self.results = ZomatoGold('ajman').results
class UmmAlQuwain:
    def __init__(self):
        self.results = ZomatoGold('umm-al-quwain').results
class AlAin:
    def __init__(self):
        self.results = ZomatoGold('al-ain').results
class RasAlKhaimah:
    def __init__(self):
        self.results = ZomatoGold('ras-al-khaimah').results