from datetime import datetime

class CommentQuery(object):
    def __init__(self, client):
        self.db = client["movieFlix"]
        self.cmnts = self.db["comments"]
        self.movs = self.db["movies"]

    def top_n_commenters(self, N:int):
        pipeline = [
            { "$group": { "_id": "$name", "count": { "$sum": 1 } } },
            { "$sort": { "count": -1 } },
            { "$limit": N }
        ]

        cursor = self.cmnts.aggregate(pipeline)

        for user_data in cursor:
            user_name = user_data["_id"]
            comment_count = user_data["count"]
            print(f"User: {user_name}, Comment Count: {comment_count}")

    def top_n_commented_movies(self,N: int):
        pipeline = [
            {
                "$lookup": {
                    "from": "movies",  # Reference "movies" collection
                    "localField": "movie_id",  # Foreign key in comments
                    "foreignField": "_id",  # Primary key in movies
                    "as": "movie"  # Alias for joined movie data
                }
            },
            { "$unwind": "$movie" },  # Deconstruct the "movie" array
            {
                "$group": {
                    "_id": "$movie_id",  # Group by movie_id (foreign key)
                    "comment_count": { "$sum": 1 },  # Count comments for each movie
                    "title": { "$first": "$movie.title" }  # Get title from first joined movie doc
                }
            },
            { "$sort": { "comment_count": -1 } },  # Sort by comment count (descending)
            { "$limit": N }  # Limit to top N movies
        ]

        # Execute aggregation on comments collection
        cursor = self.cmnts.aggregate(pipeline)

        # Print results (replace with your preferred output method)
        for movie_data in cursor:
            movie_id = movie_data["_id"]
            comment_count = movie_data["comment_count"]
            title = movie_data["title"]
            print(f"Title: {title}, Comment Count: {comment_count}")

    def comment_count_by_month_in_yr(self,year: int):
        pipeline = [
            {
                "$match": {
                    "date": {
                        "$gte": datetime(year, 1, 1),  # Start of year (inclusive)
                        "$lt": datetime(year + 1, 1, 1),  # End of year (exclusive)
                    }
                }
            },
            {
                "$project": {
                    "month": { "$month": "$date" },  # Extract month from date
                    "count": 1  # Add a count of 1 for each comment
                }
            },
            {
                "$group": {
                    "_id": "$month",  # Group by month
                    "count": {"$sum": 1}  # Sum comment counts for each month
                }
            },
            { "$sort": { "_id": 1 } }  # Sort by month (ascending)
        ]

        cursor = self.cmnts.aggregate(pipeline)

        res = {i:0 for i in range(1,13)}
        for month_data in cursor:
            month = month_data["_id"]
            comment_count = month_data["count"]
            res[month] += comment_count

        for k in res:
            print(f"Month {k}: {res[k]} comments")

# top_n_commenters(5)
# top_n_commented_movies(5)
# comment_count_by_month_in_yr(2000)