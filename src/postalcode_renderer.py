import geopandas as gpd
import matplotlib.pyplot as plt
import plotly.express as px
import requests
import logging
import pandas as pd
import os

logging.basicConfig(filename='etl_postal_code.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class PostalCodeRenderer:
    """
    This class provides methods to render postal codes data.
    """ 
    
    def __init__(self, country) -> None:
        self.logger = logging.getLogger(f'{PostalCodeRenderer.__name__}')
        self.save_dir = "data/"
        self.country = country
        
    def render_nearby_postalcodes(self, reference_postal_code, nearby_postal_codes, df):
        """
        Render a static map showing postal codes, nearby postal codes, and a reference postal code.

        Parameters:
        - reference_postal_code (str): The reference postal code.
        - nearby_postal_codes (pd.Series): A Series of nearby postal codes.
        - df (pd.DataFrame): DataFrame containing postal code data with columns 'PostalCode', 'Latitude', and 'Longitude'.
        """
        # Load the GeoJSON file for a country
        country_geodata = gpd.read_file(f'{self.country}_bundeslaender.geo.json')

        # Plot the background sketch of a country
        ax = country_geodata.plot(figsize=(10, 6), color='lightgray', edgecolor='black')

        # Scatter plot for postal codes
        ax.scatter(df['Longitude'], df['Latitude'], label='Postal Codes', color='blue')

        # Highlight nearby postal codes and reference postal code
        ax.scatter(df[df['PostalCode'].isin(nearby_postal_codes)]['Longitude'],
                df[df['PostalCode'].isin(nearby_postal_codes)]['Latitude'],
                label='Nearby Postal Codes', color='red', marker='x')

        ax.scatter(df[df['PostalCode'] == reference_postal_code]['Longitude'],
                df[df['PostalCode'] == reference_postal_code]['Latitude'],
                label='Reference Postal Code', color='green', marker='o')

        plt.title(f'Postal Codes and Nearby Postal Codes in {self.country}')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.legend()
        plt.show()
        
    def render_nearby_postalcodes_interactive(self, reference_postal_code: str, nearby_postal_codes: pd.Series):
        """
        Render an interactive map showing postal codes, nearby postal codes, and a reference postal code.

        Parameters:
        ----------
            - reference_postal_code (str): The postal code of the reference place.
            - nearby_postal_codes (pd.Series): A series of postal codes that are within a certain distance from the reference place.
        
        Reads the CSV file that contains the postal codes and their coordinates (`'{self.save_dir}{self.country}_postal_codes_and_coordinates.csv'`).
        If the file does not exist, prints an error message and returns None.
        Loads the GeoJSON file for the country ('{self.country}_bundeslaender.geo.json') and creates a choropleth map of the postal codes.
        Adds scatter plots for the postal codes, the nearby postal codes, and the reference postal code with different colors and titles.
        Updates the layout of the map and shows it in a new window.
        
        """
        
        coordinates_file = f'{self.save_dir}{self.country}_postal_codes_and_coordinates.csv'
        if not os.path.exists(coordinates_file):
            msg = f"The file '{coordinates_file}' does not exists. Get coordinates for postal codes"
            self.logger.error(msg)
            print(msg)
            return
        
        postal_codes_df = pd.read_csv(coordinates_file, dtype={'PostalCode': str})
        postal_codes_df.dropna(subset=["Longitude", 'Latitude'], axis=0, how='any', inplace=True)
        # Load the GeoJSON file for Germany
        country_geodata = gpd.read_file(f'{self.country}_bundeslaender.geo.json')

        # Create a choropleth map for Germany
        fig = px.choropleth_mapbox(postal_codes_df, geojson=country_geodata.geometry, locations=postal_codes_df['PostalCode'],
                                    featureidkey="properties.postalcode",
                                    center={"lat": postal_codes_df['Latitude'].mean(), "lon": postal_codes_df['Longitude'].mean()},
                                    mapbox_style="carto-positron", zoom=5)

        # Scatter plot for postal codes
        fig.add_trace(px.scatter_mapbox(postal_codes_df, lat='Latitude', lon='Longitude', hover_data=['PostalCode'],
                                        color_discrete_sequence=['blue'],
                                        title='Postal Codes').data[0])

        # Highlight nearby postal codes and reference postal code
        fig.add_trace(px.scatter_mapbox(postal_codes_df[postal_codes_df['PostalCode'].isin(nearby_postal_codes)],
                                        lat='Latitude', lon='Longitude', hover_data=['PostalCode'],
                                        color_discrete_sequence=['red'],
                                        title='Nearby Postal Codes').data[0])

        fig.add_trace(px.scatter_mapbox(postal_codes_df[postal_codes_df['PostalCode'] == reference_postal_code],
                                        lat='Latitude', lon='Longitude', hover_data=['PostalCode'],
                                        color_discrete_sequence=['green'],
                                        title='Reference Postal Code').data[0])

        fig.update_layout(title_text=f'Postal Codes and Nearby Postal Codes in {self.country}')
        # fig.to_image('png')
        fig.show()

    def render_country_borders(country_name):
        """
        Render a map showing the borders of a specific country.

        Parameters:
        - country_name (str): The name of the country.
        """
        # Load the Natural Earth countries dataset
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

        # Filter the dataset to include only the specified country
        country = world[world['name'] == country_name]

        # Plot the background sketch of the country
        ax = country.plot(figsize=(10, 6), color='lightgray', edgecolor='black')

    def get_germany_states_geojson():
        """
        Retrieve GeoJSON data for Germany states using the Overpass API.

        Returns:
        - germany_states_geojson (dict): GeoJSON data for Germany states.
        """
        # Overpass API URL for Germany states
        overpass_url = "https://overpass-api.de/api/interpreter"

        # Overpass query for Germany states (administrative boundary relation)
        overpass_query = """
            [out:json];
            area["ISO3166-1"="DE"][admin_level=2];
            (relation(area)["boundary"="administrative"]["admin_level"="4"];
            );
            out geom;
        """

        # Send a POST request to the Overpass API
        response = requests.post(overpass_url, data=overpass_query)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON data
            germany_states_geojson = response.json()

            return germany_states_geojson

        # Print an error message if the request was not successful
        print(f"Failed to retrieve GeoJSON data. Status code: {response.status_code}")
        return None

    # # Example usage
    # germany_states_geojson = get_germany_states_geojson()

    # # Save the GeoJSON data to a file
    # if germany_states_geojson:
    #     with open("germany_states_nominatim.geojson", "w") as geojson_file:
    #         json.dump(germany_states_geojson, geojson_file, indent=2)

    #     print("GeoJSON data saved to germany_states_nominatim.geojson")
