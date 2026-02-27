import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import time

class DataLoader:

    def __init__(self, db_address, db_username, db_password):
        """
        Connect to the Neo4j database and other init steps
        
        Args:
            db_address (str): URI of the Neo4j database
            db_username (str): Username of the Neo4j database
            db_password (str): Password of the Neo4j database
        """
        self.db_connection = GraphDatabase.driver(db_address, auth=(db_username, db_password), encrypted=False)
        self.db_connection.verify_connectivity()

    def close(self):
        """
        Close the connection to the Neo4j database
        """
        self.db_connection.close()

    def load_transform_file(self, file_path):
        """
        Load the parquet file and transform it into a csv file
        Then load the csv file into neo4j

        Args:
            file_path (str): Path to the parquet file to be loaded
        """
        trip_data = pq.read_table(file_path)
        trip_data = trip_data.to_pandas()

        trip_data = trip_data[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 
                              'PULocationID', 'DOLocationID', 
                              'trip_distance', 'fare_amount']]

        # Filter out trips
        bronx_zone_ids = [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 
                          94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 
                          183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 
                          241, 242, 247, 248, 250, 254, 259]
        
        trip_data = trip_data[trip_data.iloc[:, 2].isin(bronx_zone_ids) & 
                    trip_data.iloc[:, 3].isin(bronx_zone_ids)]
        trip_data = trip_data[trip_data['trip_distance'] > 0.1]
        trip_data = trip_data[trip_data['fare_amount'] > 2.5]

        # Convert date-time columns to supported format
        trip_data['tpep_pickup_datetime'] = pd.to_datetime(
            trip_data['tpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
        trip_data['tpep_dropoff_datetime'] = pd.to_datetime(
            trip_data['tpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')

        # Upload data to Neo4j
        with self.db_connection.session() as db_session:
            for _, trip_record in trip_data.iterrows():
                pickup_location = trip_record['PULocationID']
                dropoff_location = trip_record['DOLocationID']

                # Create pickup and dropoff location nodes
                db_session.run("MERGE (p:Location {name: $pickup_spot})", pickup_spot=pickup_location)
                db_session.run("MERGE (d:Location {name: $dropoff_spot})", dropoff_spot=dropoff_location)

                db_session.run(
                    """
                    MATCH (p:Location {name: $pickup_spot}), (d:Location {name: $dropoff_spot})
                    CREATE (p)-[r:TRIP {
                        distance: $trip_distance, 
                        fare: $trip_fare, 
                        pickup_dt: $pickup_datetime, 
                        dropoff_dt: $dropoff_datetime
                    }]->(d)
                    """,
                    pickup_spot=pickup_location,
                    dropoff_spot=dropoff_location,
                    trip_distance=trip_record['trip_distance'],
                    trip_fare=trip_record['fare_amount'],
                    pickup_datetime=trip_record['tpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                    dropoff_datetime=trip_record['tpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S')
                )

def main():
    max_attempts = 10
    attempt_count = 0

    # The database takes some time to start up!
    while attempt_count < max_attempts:
        try:
            loader = DataLoader("neo4j://localhost:7687", "neo4j", "project1phase1")
            loader.load_transform_file("yellow_tripdata_2022-03.parquet")
            loader.close()
            attempt_count = max_attempts
        except Exception as error_msg:
            print(f"(Attempt {attempt_count+1}/{max_attempts}) Error: ", error_msg)
            attempt_count += 1
            time.sleep(10)

if __name__ == "__main__":
    main()
