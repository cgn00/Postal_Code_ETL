import pandas as pd
import time

from src.postalcode_extraction import PostalCodeExtraction
from src.postalcode_transformation import PostalCodeTransform
from src.postalcode_loader import PostalCodeLoader
from src.postalcode_renderer import PostalCodeRenderer

def main():
    """
    Main script to demonstrate the usage of postal code functions and rendering nearby postal codes.
    """
    
    country = 'germany'
    extractor = PostalCodeExtraction(country)
    start = time.time()
    # extractor.scrape_cities()
    # extractor.scrape_postals_code()
    # country_pc = extractor.merge_postalcodes()
    end = time.time()
    print(f"Time taken to run the code was {end-start} seconds")

    # List of postal codes for which we want to obtain coordinates
    postal_codes_by_city = [
        '78267',
        '01824',
        '50667',
        '51149',
        '06420',
        '73430',
        '73431',
        '73432',
        '73433',
        '73434',
    ]
    
    # german_cities_codes = pd.read_csv('german_cities_with_codes.csv')
    
    # postal_codes_by_city = german_cities_codes['PostalCode']
    
    transforms = PostalCodeTransform(country)
    transforms.clean_postal_codes()
    transforms.split_cities_postal_codes()
    start = time.time()
    postal_codes_coordinates_df = transforms.get_coordinates(8)
    end = time.time()
    print(f"Time taken to run the code was {end-start} seconds")
    
    # postal_codes_by_city = transforms.clean_postal_codes(codes=postal_codes_by_city)
    # german_cities_codes['PostalCode'] = postal_codes_by_city
    
    # postal_codes_df = transforms.get_post_codes(german_cities_codes)
    # post_codes_array = postal_codes_df['PostalCode'].values

    # coordinates = transforms.get_coordinates(postal_codes=post_codes_array, country="de")
    
    # Get coordinates for each postal code using the geocoder
    # coordinates = transforms.get_coordinates(post_codes_array, country)

    # Print the coordinates for each postal code
    # for i, postal_code in enumerate(postal_codes_df['Code']):
    #     print(f'Postal code {postal_code} -> Longitude: {coordinates[0][i]}, Latitude: {coordinates[1][i]}')
    # postal_codes_df = pd.merge(postal_codes_df, coordinates, on='PostalCode', how='left')
    
    # postal_codes_df.to_csv('postal_codes_and_coordinates.csv', index=False)
    
    # non_nan_coordinates = postal_codes_df.dropna(subset=["Longitude", 'Latitude'], axis=0, how='any')
    # postal_codes_df = pd.read_csv('postal_codes_and_coordinates.csv')
    
    # # Create a DataFrame to store the postal codes, longitude, and latitude
    # df = pd.DataFrame()
    # df['PostalCode'] = postal_codes
    # df['Longitude'] = [coord[0] for coord in coordinates]
    # df['Latitude'] = [coord[1] for coord in coordinates]

    # Choose a reference postal code and a radius in kilometers
    reference_postal_code = str(postal_codes_coordinates_df.loc[0, 'PostalCode'])
    radius_km = 50

    # Find nearby postal codes within the specified radius
    nearby_postal_codes = PostalCodeLoader(country).find_nearby_postal_codes_by_distance(reference_postal_code=reference_postal_code, radius_km=radius_km)

    # Print the nearby postal codes and render an interactive map
    if (nearby_postal_codes is not None):
        if nearby_postal_codes.any():
            print(f"Nearby Postal Codes: {', '.join(nearby_postal_codes.to_list())}")
            PostalCodeRenderer(country).render_nearby_postalcodes_interactive(reference_postal_code, nearby_postal_codes)
    else:
        print("No nearby postal codes found.")
        
    end = time.time()
    print(f"Time taken to run the code was {(end-start)/60} minutes")
    
if __name__ == "__main__":
    main()