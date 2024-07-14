class MovieQuery(object):
    def __init__(self,client):
        self.db = client["movieFlix"]
        self.collection = self.db["movies"]

    # Build the pipeline stages
    def top_n_imdb_ratings(self,N: int):
        pipeline = [
            {"$match": {"imdb.rating" : {"$ne":""}}},
            { "$sort": { "imdb.rating": -1 } },
            { "$limit": N },
            { "$project": { "title": 1, "imdb.rating": 1 } }

        ]
        cursor = self.collection.aggregate(pipeline)

        for movie in cursor:
            print(f"Title: {movie['title']} ; Rating: {movie['imdb']['rating']}")

    def top_n_imdb_ratings_in_yr(self,N: int, yr: int):
        pipeline = [
            {"$match": {"imdb.rating" : {"$ne":""}}},
            {"$match" : {"year": yr }},
            { "$sort": { "imdb.rating": -1 } },
            { "$limit": N },
            { "$project": { "title": 1, "imdb.rating": 1 , "year": 1} }

        ]
        cursor = self.collection.aggregate(pipeline)

        for movie in cursor:
            print(f"Title: {movie['title']} ; Rating: {movie['imdb']['rating']}")

        return

    def top_n_imdb_ratings_1000voted(self,N: int):
        pipeline = [
            {"$match": {"imdb.rating" : {"$ne":""}}},
            { "$match": {"imdb.votes": {"$gt": 1000}}},
            { "$sort": { "imdb.rating": -1 } },
            { "$limit": N },
            { "$project": { "title": 1, "imdb.rating": 1 , "imdb.votes": 1} }
        ]
        cursor = self.collection.aggregate(pipeline)

        for movie in cursor:
            print(f"Title: {movie['title']} ; Rating: {movie['imdb']['rating']} ; Votes: {movie['imdb']['votes']}")

    def top_n_match_title(self,N: int, search_pattern: str):
        pipeline = [
            {"$match": {"imdb.rating" : {"$ne":""}}},
            {"$match": {"title": { "$regex": search_pattern}}},
            {"$sort": { "tomatoes.viewer.rating": -1 }},
            { "$limit": N },
            { "$project": { "title": 1, "tomatoes.viewer.rating": 1 } }
        ]

        # Execute the aggregation with pymongo
        cursor = self.collection.aggregate(pipeline)

        # Print the results (replace with your preferred output method)
        for movie in cursor:
            print(f"Title: {movie['title']}, Tomatoes Rating: {'NA' if movie.get('tomatoes',None)==None else movie['tomatoes']['viewer']['rating']}")



# top_n_imdb_ratings(10)
# top_n_imdb_ratings_in_yr(10,1947)
# top_n_imdb_ratings_1000voted(100)
# top_n_match_title(10, "bee")
