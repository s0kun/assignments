class DirectorQuery(object):
    def __init__(self,client):
        self.db = client["movieFlix"]
        self.movies = self.db["movies"]
        self.users = self.db["users"]

    def top_n_exprnced_dirs(self,N: int):
        pipeline = [
            { "$unwind": "$directors" },
            # {
            #     "$lookup": {
            #         "from": users.name,
            #         "localField": "directors",
            #         "foreignField": "name",
            #         "as": "director_data"
            #     }
            # },
            # { "$match": { "director_data.0": { "$exists": True } } },
            { "$group": { "_id": "$directors", "movie_count": { "$sum": 1 } } },
            { "$sort": { "movie_count": -1 } },
            { "$limit": N }
        ]
        cursor = self.movies.aggregate(pipeline)

        for director in cursor:
            print(f"Director: {director['_id']}, Movie Count: {director['movie_count']}")

        return

    def top_n_exprnced_dirs_in_yr(self,N: int, yr: int):
        pipeline = [
            { "$unwind": "$directors" },
            { "$match": { "year": yr } },
            { "$group": { "_id": "$directors", "movie_count": { "$sum": 1 } } },
            { "$sort": { "movie_count": -1 } },
            { "$limit": N }
        ]
        cursor = self.movies.aggregate(pipeline)

        for director in cursor:
            print(f"Director: {director['_id']}, Movie Count: {director['movie_count']}")

    def top_n_exprnced_dirs_in_genre(self,N: int, gnr: str):
        pipeline = [
            { "$unwind": "$genres" },
            { "$unwind": "$directors" },
            { "$match": { "genres": gnr } },
            { "$group": { "_id": {"director": "$directors","genre": "$genres"}, "movie_count": { "$sum": 1 } } },
            { "$sort": { "movie_count": -1 } },
            { "$limit": N }
        ]
        cursor = self.movies.aggregate(pipeline)

        for director in cursor:
            print(f"Director: {director['_id']}, Movie Count: {director['movie_count']}")


# top_n_exprnced_dirs(10)
# top_n_exprnced_dirs_in_yr(10,1986)
# top_n_exprnced_dirs_in_genre(10,"Comedy")
