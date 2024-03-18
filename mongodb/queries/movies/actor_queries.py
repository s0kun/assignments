class ActorQuery(object):
    def __init__(self,client):
        self.db = client["movieFlix"]
        self.movies = self.db["movies"]
        self.users = self.db["users"]

    def top_n_exprnced_cast(self,N: int):
        pipeline = [
            { "$unwind": "$cast" },
            {
                "$group": {
                    "_id": "$cast",
                    "movie_count": { "$sum": 1 }
                }
            },
            { "$sort": { "movie_count": -1 } },
            { "$limit": N }
        ]
        cursor = self.movies.aggregate(pipeline)

        for doc in cursor:
            print(f"Actor: {doc['_id']} ; Movie Count: {doc['movie_count']}")

        return

    def top_n_exprnced_actor_in_yr(self,N: int, yr: int):
        pipeline = [
            { "$match": { "year": yr } },
            { "$unwind": "$cast" },
            { "$group": { "_id": "$cast", "movie_count": { "$sum": 1 } } },
            { "$sort": { "movie_count": -1 } },
            { "$limit": N }
        ]
        cursor = self.movies.aggregate(pipeline)

        for director in cursor:
            print(f"Director: {director['_id']}, Movie Count: {director['movie_count']}")

    def top_n_exprnced_actor_in_genre(self,N: int, gnr: str):
        pipeline = [
            { "$unwind": "$genres" },
            { "$unwind": "$cast" },
            { "$match": { "genres": gnr } },
            { "$group": { "_id": {"actor": "$cast","genre": "$genres"}, "movie_count": { "$sum": 1 } } },
            { "$sort": { "movie_count": -1 } },
            { "$limit": N }
        ]
        cursor = self.movies.aggregate(pipeline)

        for director in cursor:
            print(f"Director: {director['_id']}, Movie Count: {director['movie_count']}")

    def top_N_genre_imdb(self,N: int):
        pipeline = [
            { "$unwind": "$genres" },
            {"$match": {"imdb.rating" : {"$ne":""}}},
            {
                "$sort": {
                    "genres": 1,
                    "imdb.rating": -1
                }
            },
            {
                "$group": {
                    "_id": "$genres",
                    "movies": { "$push": "$$ROOT" }
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "top_movies": { "$slice": [ "$movies", 0, N ] }
                }
            },
            { "$project": {
                "_id": 1,
                "top_movies": {
                    "$map": {
                        "input": "$top_movies",
                        "as": "movie",
                        "in": { "title": "$$movie.title", "rating": "$$movie.imdb.rating" }
                    }
                }
            } }
        ]

        cursor = self.movies.aggregate(pipeline)

        for genre_data in cursor:
            genre = genre_data["_id"]
            top_movies = genre_data["top_movies"]
            print(f"\nGenre: {genre}")
            for movie in top_movies:
                print(f"\t- Title: {movie['title']}, Rating: {movie['rating']}")



# top_n_exprnced_cast(10)
# top_n_exprnced_actor_in_yr(19,1986)
# top_n_exprnced_actor_in_genre(10, "Comedy")