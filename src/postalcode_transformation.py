import os
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import csv
import re
from unidecode import unidecode

import logging

import pgeocode
import pycountry
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from geopy.distance import geodesic

logging.basicConfig(filename='etl_postal_code.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class PostalCodeTransform:
    """
    This class provides methods for transforming postal codes data.
    """ 
    
    def __init__(self, country) -> None:
        """
        Args:
        
            - country (str): The country where the postal codes belong.
        """
        self.logger = logging.getLogger(f'{PostalCodeTransform.__name__}')
        self.save_dir = "data/"
        self.country = country
        self.country_code = pycountry.countries.get(name=country).alpha_2
        if self.country_code is None:
            raise KeyError("Unknown country")

    def clean_postal_codes(self, postal_codes_file: str = ""):
        """
        Cleans the postal codes of the cities in a given country.
        
        Args:
        ----
            - postal_codes_file (str): The path of the CSV file that contains the postal codes of the cities. Default is ``"{self.save_dir}{self.country}_cities_postalcodes.csv"``

        """
        cleaned_file = f"{self.save_dir}{self.country}_cities_cleaned_postalcodes.csv"
        
        if os.path.exists(cleaned_file):
            
            self.logger.info(f"The file '{cleaned_file}' exists. Nothing to clean")
            print(f"The file '{cleaned_file}' exists. Nothing to clean") 
        
        else: 
            
            if postal_codes_file == "":
                postal_codes_file = f"{self.save_dir}{self.country}_cities_postalcodes.csv"
            
            postal_codes_df = pd.read_csv(postal_codes_file)     
            postal_codes_df["PostalCode"] = postal_codes_df["PostalCode"].apply(self._str_to_list)
            # postal_codes_df["PostalCode"] = postal_codes_df["PostalCode"].str.replace('', "")
            
            postal_codes_df.to_csv(cleaned_file, index=False, quoting=csv.QUOTE_MINIMAL)
            self.logger.info("Postal codes were cleaned up")
            print("Postal codes were cleaned up") 
            
        return     
    
    # def get_post_codes(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Add an additional row to a pandas dataframe for each postal code in the 'PostalCode' column if it has more than one element.
        
    #     Args:
    #     ----
    #         - df: A pandas dataframe.
        
    #     Returns:
    #     -------
    #     A pandas dataframe with an additional row for each postal code in the 'PostalCode' column if it has more than one element.
    #     """
    #     # Create a new dataframe to hold the additional rows
    #     new_df = pd.DataFrame(columns=df.columns)

    #     # Iterate over each row in the original dataframe
    #     for index, row in df.iterrows():
    #         # Check if the 'PostalCode' column has more than one element
    #         # if len(row['PostalCode']) > 1:
    #             # Add a new row to the new dataframe for each postal code in the 'PostalCode' column
    #         for code in row['PostalCode']:
    #             new_row = row.copy()
    #             new_row['PostalCode'] = code
    #             new_df.loc[len(new_df)] = new_row
        
    #         # Add the current row to the new dataframe
    #         # new_df = pd.concat([new_df, pd.DataFrame([new_row])], ignore_index=True)
    #     # Return the new dataframe
    #     return new_df
    
    def split_cities_postal_codes(self):
        """
        Splits the postal codes of the cities in a given country into separate rows.
        
        Reads the CSV file that contains the postal codes of the cities in `{self.save_dir}{self.country}_cities_cleaned_postalcodes.csv`,
        then splits the postal codes ('PostalCode' of the CSV) that are in a list format into individual values and creates a new row for each postal code.
        
        Finally saves the resulting DataFrame in `{self.save_dir}{self.country}_cities_splitted_postalcodes.csv`. If the file already exists, prints a message and does nothing.
        """
        
        cities_post_code_file = f"{self.save_dir}{self.country}_cities_splitted_postalcodes.csv"
        
        if os.path.exists(cities_post_code_file):
            
            self.logger.info(f"The file '{cities_post_code_file}' exists. Nothing to clean")
            print(f"The file '{cities_post_code_file}' exists. Nothing to clean") 
        
        else:
            cleaned_postalcodes_file = f"{self.save_dir}{self.country}_cities_cleaned_postalcodes.csv"
            df = pd.read_csv(cleaned_postalcodes_file)
            df['PostalCode'] = df['PostalCode'].str.strip(" '[]").astype(str).str.split(',')
            
            # Explode the 'PostalCode' column to create a new row for each postal code
            new_df = df.explode('PostalCode')
            new_df.drop_duplicates(subset=['PostalCode'], inplace=True)
            new_df['PostalCode'] = new_df['PostalCode'].str.strip(' ').str.replace("'", "")
            
            new_df.to_csv(cities_post_code_file, index=False)

        return 

    def geocode_postal_code(self, row):
        geolocator = Nominatim(user_agent="my-code_finder")  # Replace with your user agent
        # Retry up to 3 times in case of timeout or 429 status code
        retry_delay = 5
        row = row[1]
        postal_code = row["PostalCode"]
        # print(f'{postal_code}, {row["City"]}')
        for _ in range(3):
            try:
                location = geolocator.geocode(f'{postal_code}, {row["City"]}', country_codes=self.country_code, timeout=10)
                if location:
                    return postal_code, location.longitude, location.latitude
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                if 'HTTP Error 503' in str(e):
                    print(f"HTTP 503 error: Service temporarily unavailable. Retrying in {retry_delay} seconds.")
                    time.sleep(retry_delay)
                else:
                    print(f"Error geocoding {postal_code}: {e}")
                    # Handle other geocoding errors as needed
                    return postal_code, None, None
            else:
                self.logger.warning(f"No results found for postal code: {postal_code}")
                return postal_code, None, None
    
    def get_coordinates(self, max_workers=4) -> pd.DataFrame:
        """
        Get the coordinates (longitude, latitude) for a list of postal codes in a specified country.
        
        Args:
        ----
            - max_workers (int): The maximum number of worker threads to use for parallel processing. Default is 4.

        Returns:
            - pd.DataFrame: A DataFrame with the postal codes and their corresponding coordinates.
            
        Reads the CSV file that contains the postal codes of the cities (`"{self.save_dir}{self.country}_cities_splitted_postalcodes.csv"`).
        Uses the geocode_postal_code method to get the coordinates for each postal code using multithreading.
        Merges the coordinates with the original postal codes DataFrame and saves it as a new CSV file in `"{self.save_dir}{self.country}_postal_codes_and_coordinates.csv"`.
        If the file already exists, prints a message and returns the existing DataFrame.
        If the postal codes file does not exist, prints an error message and returns None.

        """
        
        coordinates_file = f'{self.save_dir}{self.country}_postal_codes_and_coordinates.csv'
        if os.path.exists(coordinates_file):
            msg = f"The file '{coordinates_file}' exists. Coordinates were already obtained"
            self.logger.info(msg)
            print(msg)
            postal_codes_coordinates_df = pd.read_csv(coordinates_file, dtype={'PostalCode': str})
            return postal_codes_coordinates_df
        
        postal_codes_file = f"{self.save_dir}{self.country}_cities_splitted_postalcodes.csv"
        
        if not os.path.exists(postal_codes_file):
            msg = f"The file '{postal_codes_file}' does not exists. Scrape postal codes first"
            self.logger.error(msg)
            print(msg)
            return
        postal_codes_df = pd.read_csv(postal_codes_file, dtype=str)

        # Initialize an empty list to store the coordinates  
        coordinates = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            coordinates = list(executor.map(self.geocode_postal_code, postal_codes_df.iterrows()))

        # Return the list of coordinates
        coordinates_df = pd.DataFrame(coordinates, columns = ['PostalCode', 'Longitude', 'Latitude'])
        postal_codes_coordinates_df = pd.merge(postal_codes_df, coordinates_df, on='PostalCode', how='left')
        
        postal_codes_coordinates_df.to_csv(coordinates_file, index=False)
        
        return postal_codes_coordinates_df

    def get_batch_coordinates(self, postal_codes:list, country: str) -> list:
        # create a geolocator object for the country
        nomi = pgeocode.Nominatim(country)

        # query the latitude and longitude for the whole list of postal codes
        locations = nomi.query_postal_code(postal_codes)

        # extract the latitudes and longitudes as lists
        latitudes = locations.latitude.tolist()
        longitudes = locations.longitude.tolist()
        
        coordinates = [latitudes, longitudes]

        return coordinates

    def _str_to_list(self, s:str) -> list:
        """
        Convert a string of comma-separated values and ranges separated by a hyphen to a list of str.
        
        Args:
        ----
        s: A string of comma-separated values and ranges separated by a hyphen.
        
        Returns:
        -------
        A list of integers.
        """
        
        new_s = re.sub(r"[^0-9,–]", "", s) 

        s_strip = new_s.strip()
        
        # if ', ' in s:
        #     s_splited = s_strip.split(', ')
        
        # else:
        s_splited = s_strip.split(',')
        
        s_list = []
       
        for element in s_splited:
            element = element.replace(" ", "")
            if '–' in element:
                start, end = re.findall(r'\d+', element)
                s_list += [f"{int(i):05d}" for i in range(int(start), int(end)+1)]
            elif element.endswith(','):
                s_list += [element[:-1]]
            elif element == '':
                break
            else:
                s_list += [element]
            
        return s_list