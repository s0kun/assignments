class TheatreQuery(object):

    def __init__(self, client):
        self.db = client["movieFlix"]
        self.thtrs = self.db["theatres"]
        self.thtrs.create_index([("location.geo", "2dsphere")])

    def top_n_cities_most_theatres(self, N: int):
        # Pipeline to find city counts for theatres
        pipeline = [
            { "$unwind": "$location.address" },
            { "$group": { "_id": "$location.address.city", "count": { "$sum": 1 } } },
            { "$sort": { "count": -1 } },
            { "$limit": N }
        ]

        cursor = self.thtrs.aggregate(pipeline)

        for city_data in cursor:
            city = city_data["_id"]
            theatre_count = city_data["count"]
            print(f"City: {city}, Theatre Count: {theatre_count}")

    def top_n_nearby_theatres(self, N: int, x: float, y: float):
        pipeline = [
            {
                "$geoNear": {
                    "near": { "type": "Point", "coordinates": [ x , y ] },
                    "distanceField": "distance",  # Calculate distance for each theatre
                    "spherical": True,  # Assume spherical Earth for distance calculation
                    "query": {"location.geo": { "$exists": True }},  # Filter for theatres with geo coordinates
                }
            },
            { "$sort": { "distance": 1 } },
            {"$limit": N}
        ]

        # Execute aggregation on theatres collection
        cursor = self.thtrs.aggregate(pipeline)

        # Print results (replace with your preferred output method)
        for theatre_data in cursor:
            theatre_id = theatre_data["_id"]
            distance = theatre_data["distance"]  # Distance in meters (approximately)
            address = theatre_data["location"]["address"]  # Access address information
            print(f"Theatre ID: {theatre_id}, Distance: {distance:.2f} meters")
            print(f"\tAddress: {address['street1']}, {address['city']}, {address['state']}")  # Print formatted address


# top_n_cities_most_theatres(10)
# top_n_nearby_theatres(3, 0,0)