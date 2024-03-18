from bson.json_util import loads
from connector import *

class MongoAPI:

    def __init__(self, mongo_client,  database_name):
        self.client = mongo_client
        self.db = self.client[database_name]
        self.comments = self.db["comments"]
        self.movies = self.db["movies"]
        self.theatres = self.db["theatres"]
        self.users = self.db["users"]

    def insert_comment(self, comment: str):
        """
        Inserts a new comment document into the comments collection.

        Args:
            comment_data (dict): A dictionary containing the comment details.
                Expected keys: user_id, movie_id, content, timestamp (optional)

        Returns:
            ObjectId: The ObjectId of the inserted document or None if insertion fails.
        """
        comment_data = loads(comment)
        try:
            result = self.comments.insert_one(comment_data)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting comment: {e}")
            return None

    def insert_movie(self, mov: str):
        """
        Inserts a new movie document into the movies collection.

        Args:
            movie_data (dict): A dictionary containing the movie details.
                Expected keys: title, description, genre, release_date, cast (list of strings), director

        Returns:
            ObjectId: The ObjectId of the inserted document or None if insertion fails.
        """
        movie_data = loads(mov)

        try:
            result = self.movies.insert_one(movie_data)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting movie: {e}")
            return None

    def insert_theatre(self, thtr: str):
        """
        Inserts a new theatre document into the theatres collection.

        Args:
            theatre_data (dict): A dictionary containing the theatre details.
                Expected keys: name, location, screens (int)

        Returns:
            ObjectId: The ObjectId of the inserted document or None if insertion fails.
        """
        theatre_data = loads(thtr)

        try:
            result = self.theatres.insert_one(theatre_data)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting theatre: {e}")
            return None

    def insert_user(self, usr):
        """
        Inserts a new user document into the users collection.

        Args:
            user_data (dict): A dictionary containing the user details.
                Expected keys: username, email, password (hashed)

        Returns:
            ObjectId: The ObjectId of the inserted document or None if insertion fails.
        """
        user_data = loads(usr)

        try:
            result = self.users.insert_one(user_data)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting user: {e}")
            return None


client = connect()
dbName = "movieFlix"
mgr = MongoAPI(client,dbName)

