import os
import pandas as pd
import math
from geopy.distance import geodesic

import logging

logging.basicConfig(filename='etl_postal_code.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class PostalCodeLoader:
    """
    This class provides methods for loading postal codes data.
    """
    def __init__(self, country: str):
        self.logger = logging.getLogger(f'{PostalCodeLoader.__name__}')
        self.save_dir = "data/"
        self.country = country
        
    def find_nearby_postal_codes_by_bounding(self, reference_postal_code: str = None, radius_km: float = 50) -> pd.Series:
        """
        Finds the nearby postal codes within a bounding box from a reference place.

        Parameters:
        ----------
            - reference_postal_code (str): The postal code of the reference place.
            - radius_km (float): The radius in kilometers to define the bounding box.  Defaults to 50.

        Returns:
        -------
            - pd.Series: A series of postal codes that are within the bounding box from the reference place. 
            
         Reads the CSV file that contains the postal codes and their coordinates (`'{self.save_dir}{self.country}_postal_codes_and_coordinates.csv'`).
        If the file does not exist, prints an error message and returns None.
        If the reference postal code, finds the corresponding row in the DataFrame.
        Calculates the bounding box coordinates based on the reference coordinates and the radius.
        Filters the postal codes that are within the bounding box and returns them as a series.
        
        """
        coordinates_file = f'{self.save_dir}{self.country}_postal_codes_and_coordinates.csv'
        if not os.path.exists(coordinates_file):
            msg = f"The file '{coordinates_file}' does not exists. Get coordinates for postal codes"
            self.logger.error(msg)
            print(msg)
            return
        
        postal_codes_df = pd.read_csv(coordinates_file, dtype={'PostalCode': str})
        postal_codes_df.dropna(subset=["Longitude", 'Latitude'], axis=0, how='any', inplace=True)
        
        if reference_postal_code is not None:
            reference_row = postal_codes_df[postal_codes_df['PostalCode'] == reference_postal_code]
        else:
            msg = f"No reference place was provided"
            self.logger.error(msg)
            print(msg)
            return

        if reference_row.empty:
            msg = f"Reference place not found"
            self.logger.error(msg)
            print(msg)
            return

        reference_coordinates = (reference_row['Latitude'].iloc[0], reference_row['Longitude'].iloc[0])

        bounding_box = (
            reference_coordinates[0] - (radius_km / 110.574),  # Latitude degrees to kilometers
            reference_coordinates[1] - (radius_km / (111.32 * math.cos(math.radians(reference_coordinates[0])))),  # Longitude degrees to kilometers
            reference_coordinates[0] + (radius_km / 110.574),
            reference_coordinates[1] + (radius_km / (111.32 * math.cos(math.radians(reference_coordinates[0]))))
        )
        
        # Select postal codes within the bounding box
        nearby_postal_codes: pd.Series = postal_codes_df[
            (bounding_box[0] <= postal_codes_df['Latitude']) & (postal_codes_df['Latitude'] <= bounding_box[2]) &
            (bounding_box[1] <= postal_codes_df['Longitude']) & (postal_codes_df['Longitude'] <= bounding_box[3])
        ]['PostalCode']
        
        # Remove the reference postal code from the result
        nearby_postal_codes.drop(reference_row.index, inplace=True)

        return nearby_postal_codes

    def _calculate_distance(self, row, reference_latitude, reference_longitude):
        """
        Calculate the distance between a given row's coordinates and a reference point.

        Parameters:
        ----------
        - row (pd.Series): A row from the DataFrame with 'Latitude' and 'Longitude' columns.
        - reference_latitude (float): Latitude of the reference point.
        - reference_longitude (float): Longitude of the reference point.

        Returns:
        -------
        - distance (float): The distance in kilometers between the row's coordinates and the reference point.
        """
        point = (row['Latitude'], row['Longitude'])
        reference_point = (reference_latitude, reference_longitude)
        return geodesic(point, reference_point).kilometers

    def find_nearby_postal_codes_by_distance(self, reference_postal_code: str = None, reference_city: str = None, radius_km: float = 50) -> pd.Series:
        """
        Finds the nearby postal codes within a given distance from a reference postal code or city.
        
        Parameters:
        ----------
            - reference_postal_code (str): The postal code of the reference place. Defaults to None.
            - reference_city (str): The city name of the reference place. Defaults to None.
            - radius_km (float): The radius in kilometers to search for nearby postal codes. Defaults to 50.
           

        Returns:
        -------
            - pd.Series: A series of postal codes that are within the radius from the reference place.
            
        Reads the CSV file that contains the postal codes and their coordinates (`'{self.save_dir}{self.country}_postal_codes_and_coordinates.csv'`).
        If the file does not exist, prints an error message and returns None.
        If the reference postal code or city is provided, finds the corresponding row in the DataFrame.
        If the reference place is not found or not provided, prints an error message and returns None.
        Calculates the distance between the reference place and each postal code.
        Filters the postal codes that are within the radius and returns them as a series.
        """
        
        coordinates_file = f'{self.save_dir}{self.country}_postal_codes_and_coordinates.csv'
        if not os.path.exists(coordinates_file):
            msg = f"The file '{coordinates_file}' does not exists. Get coordinates for postal codes"
            self.logger.error(msg)
            print(msg)
            return
        
        postal_codes_df = pd.read_csv(coordinates_file, dtype=str)
        postal_codes_df.dropna(subset=["Longitude", 'Latitude'], axis=0, how='any', inplace=True)
        
        if reference_postal_code is not None:
            reference_row = postal_codes_df[postal_codes_df['PostalCode'] == reference_postal_code]
        elif reference_city is not None:
            reference_row = postal_codes_df[postal_codes_df['City'] == reference_city]
        else:
            msg = f"No reference place was provided"
            self.logger.error(msg)
            print(msg)
            return

        if reference_row.empty:
            msg = f"Reference place not found"
            self.logger.error(msg)
            print(msg)
            return

        reference_coordinates = (reference_row['Latitude'].iloc[0], reference_row['Longitude'].iloc[0])
        
        # Calculate distance for each row in the DataFrame
        postal_codes_df['Distance'] = postal_codes_df.apply(self._calculate_distance, axis=1, args=(reference_coordinates[0], reference_coordinates[1]))
        
        # Select postal codes within the specified distance
        nearby_postal_codes: pd.Series = postal_codes_df[(postal_codes_df['Distance'] > 0) & (postal_codes_df['Distance'] < radius_km)]['PostalCode']

        return nearby_postal_codes