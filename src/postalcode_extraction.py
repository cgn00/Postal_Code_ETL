import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import re
from unidecode import unidecode

import requests
from bs4 import BeautifulSoup

import logging

logging.basicConfig(filename='etl_postal_code.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class PostalCodeExtraction:
    """
    This class provides methods for extracting postal codes data from web pages.
    """
    
    def __init__(self, country: str):
        """
        Contains the methods to extract information of postal codes of the country recieved as parameter.

        Args:
           - country (str): The country to extract the postal codes.

    
        """
        self.logger = logging.getLogger(f'{PostalCodeExtraction.__name__}')
        self.save_dir = "data/"
        self.country = country
    
    #region Helpers
    
    # Function to remove diacritics using unidecode
    def _remove_diacritics(self, word):
        return unidecode(word).lower()
    
    def process_string(self, input_string):
        # Remove text between parentheses
        result_string = re.sub(r'\([^)]*\)', '', input_string)
        
        # Remove text after '/'
        result_string = result_string.split('/')[0].strip()
        
        return result_string
    
    #endregion

    def scrape_germany_cities(self, url: str = "https://de.wikipedia.org/wiki/Liste_der_St%C3%A4dte_in_Deutschland"): 
        """
        This function scrapes the list of cities in Germany from the German Wikipedia page and saves it to a CSV file.
        
        Parameters:
        ----------
            - url (str): The URL of the German Wikipedia page containing the list of cities. Default is "https://de.wikipedia.org/wiki/Liste_der_St%C3%A4dte_in_Deutschland".
        
        Returns:
        -------
            None

        Saves the scraped data as a CSV file in ``{self.save_dir}{self.country}_cities.csv``
        If the file already exists, prints a message and does nothing.
        
        """
        cities_file = f"{self.save_dir}{self.country}_cities.csv"
        if os.path.exists(cities_file):
            self.logger.info(f"The cities were already scraped and are saved on {cities_file}")
            print(f"The cities were already scraped and are saved on {cities_file}")
            
        else:   
            self.logger.info(f"Scraping German cities from: {url}")
            print(f"Scraping German cities from: {url}")
            # Send a GET request to the URL and get the HTML content
            response = requests.get(url)
            html = response.text

            # Parse the HTML content using Beautiful Soup
            soup = BeautifulSoup(html, "html.parser")
            
            # Find the table element that contains the regions
            regions_tablerow = soup.find("table", {"class": "wikitable"}).find_all("tr", class_ = False)
            regions = [(tr.contents[1].find("a")['title'], tr.contents[1].contents[-1].strip(" ()")) for tr in regions_tablerow]
            regions = pd.DataFrame(regions, columns=['Region', 'RegionCode'])
            
            # Find the table element that contains the list of cities
            tables = soup.findAll("table", {"class": ""})

            # Initialize an empty list to store the city names and states
            cities = []
            for table in tables:
                # Loop through the table columns
                for column in table.find_all("td"):

                    for cell in column.find_all("dd"):
                        # If the cell exists
                        if cell:
                            
                            # Find the anchor element that contains the city name and link
                            anchor = cell.find("a")

                            # Get the link attribute of the anchor element
                            link = anchor["href"]
                            
                            # Get the region code
                            code_pattern = r"[A-Z]{2}"
                            region_code = re.search(code_pattern, cell.contents[1]).group()
                            # region_code = cell.contents[1].split('(')[-1].split(")")[0].split(',')[0]
                            
                            # Get the city
                            city = self.process_string(anchor["title"])

                            # Append the city name and region code as a tuple to the list
                            cities.append((region_code, city, link))

            # Convert the list of tuples to a DataFrame
            df = pd.DataFrame(cities, columns=["RegionCode", "City", "Link"])
            df = pd.merge(regions, df, on='RegionCode')

            data_folder = f"{self.save_dir}"
            if not os.path.isdir(data_folder):
                os.makedirs(data_folder)
            # Save the DataFrame to a CSV file
            df.to_csv(cities_file, index=False)
            print(f"The cities were saved on {cities_file}")
            self.logger.info(f"The cities were saved on {cities_file}")
        return
    
    def scrape_cities(self, url: str = "https://de.wikipedia.org/wiki/Liste_der_St%C3%A4dte_in_Deutschland") -> pd.DataFrame: 
        """
        Scrape a list of cities of the country of the class, from the Wikipedia page.
        
        Saves the scraped data as a CSV file in ``{self.save_dir}{self.country}_cities.csv``
        If the file already exists, prints a message and does nothing.

        Args:
        ----    
           - url (str): The URL of the Wikipedia page to scrape. Default is "https://de.wikipedia.org/wiki/Liste_der_St%C3%A4dte_in_Deutschland".

        """
        
        country_func = getattr(PostalCodeExtraction, f'scrape_{self.country}_cities')
        result = country_func(self, url)
        return result

    def _scrape_city_postal_codes(self, row,  base_url: str = 'https://de.wikipedia.org') -> pd.Series:
        """
        This method scrapes the postal codes of a city in Germany from the German Wikipedia page and returns them as a Pandas DataFrame.
        
        Parameters:
        ---------
        row (pandas.Series): A row of a Pandas DataFrame containing the name and link of a city in Germany.
        base_url (str): The base URL of the German Wikipedia page containing the list of cities. 
        
        Example:
        -------
            >>> df[['PostalCode']] = df.apply(scrape_postal_codes, axis=1, base_url = 'https://de.wikipedia.org')
        
        """
        row = row[1] # Ignore the index
        # Print the name of the city
        self.logger.info(f"Scraping postal code of {row['City']}")

        # Construct the URL of the city's Wikipedia page
        url = base_url + row['Link']

        # Send a GET request to the URL and get the HTML content
        response = requests.get(url)
        html = response.text

        # Parse the HTML content using Beautiful Soup
        soup = BeautifulSoup(html, "html.parser")
        
        if row['City'] in ['Berlin', 'Hamburg']:            
            table = soup.find("table")

        else:
            table = soup.find("table", {"class": "hintergrundfarbe5 float-right toptextcells infobox"})
            
        # Find the table element that contains the postal code of the city
        # table = soup.find("table", {"class": "hintergrundfarbe5 float-right toptextcells infobox"})
        
        if table:
            # Find the anchor element that contains the postal code
            cell = table.find("a", {"title": "Postleitzahl (Deutschland)"})
            # Get the postal code from the next element
            next_cell = cell.findNext()
            content = next_cell.contents[0].text
            str_code = content.strip('\n') #.text.

        # Return a Pandas Series containing the postal code of the city
        return pd.Series({'PostalCode': str_code})
    
    def scrape_postals_code(self, base_url: str = 'https://de.wikipedia.org'):
        """
        This method scrapes the postal codes of cities in `self.country` from the `base_url` page and saves them on ``"{self.save_dir}{self.country}_cities_postalcodes.csv"``.
        
        Parameters:
        ----------
            - base_url (str): The base URL of the German Wikipedia page to concat with the pages of the cities. Default is 'https://de.wikipedia.org'.
        
        """
        
        postal_codes_file = f"{self.save_dir}{self.country}_cities_postalcodes.csv"
               
        if os.path.exists(postal_codes_file):
            # postal_codes_df = pd.read_csv(postal_codes_file)
            self.logger.info(f"The file '{postal_codes_file}' exists. Postal codes were already scraped")
            print(f"The file '{postal_codes_file}' exists. Postal codes were already scraped")
        else:        
            cities_file = f"{self.save_dir}{self.country}_cities.csv"
            if os.path.exists(cities_file):
                msg = f"Scraping postal codes from {self.country}"
                print(msg)
                self.logger.info(msg)
                cities_df = pd.read_csv(cities_file)
                with ThreadPoolExecutor() as executor:
                    results = list(executor.map(self._scrape_city_postal_codes, cities_df.iterrows()))
                cities_df[['PostalCode']] = results
                cities_df.to_csv(postal_codes_file, index=False)
                print(f"Postal codes were saved on {postal_codes_file}")
                self.logger.info(f"Postal codes were saved on {postal_codes_file}")
            else:
                print(f"The file '{cities_file}' does not exists. Run first .scrape_cities() method")
                self.logger.info(f"The file '{cities_file}' does not exists. Run first .scrape_cities() method")

        return 

    def get_regions(self, country_url: str):
        response = requests.get(country_url)
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        regions_html = soup.find("div", {"class": "regions"}).find_all("a")
        regions = [(region.text, region["href"]) for region in regions_html]
        return regions
    
    def get_postal_codes(self, base_url: str, region_url: str):
        response = requests.get(f'{base_url}{region_url}')
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        cities_html = soup.find_all("div", {"class": "container"})
        cities_postcodes = [(region_url,
                             city.find("div", {"class": "place"}).text, 
                             city.find("div", {"class": "code"}).text) for city in cities_html]
        return cities_postcodes
    
    def scrape_postal_codes(self):
        
        base_url = 'https://worldpostalcode.com'
        country_url = f'{base_url}/{self.country}/'
        regions = self.get_regions(country_url)
        country_postalcodes = pd.DataFrame(columns=['Region', 'Place', 'PostalCode'])
        region_codes = []
        for region in regions:
            region_codes += self.get_postal_codes(base_url, region[1])
        regions = pd.DataFrame(regions, columns=['Region', 'RegionURL'])
        region_codes = pd.DataFrame(region_codes, columns=['RegionURL', 'Place', 'PostalCode'])
        country_postalcodes = pd.merge(regions, region_codes, on='RegionURL')
        
        country_postalcodes.to_csv(f'{self.save_dir}{self.country}_postalcodes.csv', index=False)
        
        return country_postalcodes
    
    def merge_postalcodes(self):
        
        # Filter place by cities
        country_cities = pd.read_csv(f'{self.save_dir}{self.country}_cities.csv')
        country_postalcodes = pd.read_csv(f'{self.save_dir}{self.country}_postalcodes.csv')
        country_cities["Region_NoDiacritics"] = country_cities["Region"].apply(self._remove_diacritics)
        country_postalcodes["Region_NoDiacritics"] = country_postalcodes["Region"].apply(self._remove_diacritics)
        country_cities["Place_NoDiacritics"] = country_cities["City"].apply(self._remove_diacritics)
        country_postalcodes["Place_NoDiacritics"] = country_postalcodes["Place"].apply(self._remove_diacritics)
        
        country_postalcodes = pd.merge(country_cities, country_postalcodes, how='left', on=['Region_NoDiacritics', 'Place_NoDiacritics'])
        # country_postalcodes.drop(['Region_NoDiacritics_x', 'Place_NoDiacritics_x'])
        country_postalcodes.to_csv(f'{self.save_dir}{self.country}_cities_postalcodes.csv', index=False)