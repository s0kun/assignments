from connector import *
from bson import ObjectId  # Used for validating ObjectIds
from datetime import datetime,time
import bcrypt

def create_movie_document():
    """
    Prompts user for movie information and converts it to a JSON document.
    """
    movie = {}

    # Get basic information
    movie["title"] = input("Enter movie title: ")
    movie["plot"] = input("Enter movie plot summary: ")
    genres = input("Enter genres (comma separated): ").split(",")
    movie["genres"] = [genre.strip() for genre in genres]  # Remove leading/trailing spaces
    movie["runtime"] = int(input("Enter movie runtime in minutes: "))
    cast = input("Enter cast members (comma separated): ").split(",")
    movie["cast"] = [actor.strip() for actor in cast]  # Remove leading/trailing spaces
    try:
        release_date_str = input("Enter release date (YYYY-MM-DD): ")
        release_date = datetime.isoformat(datetime.combine(datetime.strptime(release_date_str, "%Y-%m-%d").date(),time.min))
        movie["released"] = release_date
    except ValueError:
        print("Invalid release date format. Please enter YYYY-MM-DD.")
        return None

    # Optional information
    movie["fullplot"] = input("Enter full plot description (optional): ")
    languages = input("Enter languages (comma separated, optional): ").split(",")
    movie["languages"] = [lang.strip() for lang in languages] if languages else []
    directors = input("Enter directors (comma separated, optional): ").split(",")
    movie["directors"] = [director.strip() for director in directors] if directors else []
    writers = input("Enter writers (comma separated, optional): ").split(",")
    movie["writers"] = [writer.strip() for writer in writers] if writers else []
    awards_text = input("Enter awards information (optional): ")
    if awards_text:
        try:
            wins = int(input("Enter number of awards won (optional): "))
            nominations = int(input("Enter number of award nominations (optional): "))
            movie["awards"] = {"wins": wins, "nominations": nominations, "text": awards_text}
        except ValueError:
            print("Invalid input for awards. Skipping...")
    imdb_info = {}
    try:
        imdb_rating = float(input("Enter IMDb rating (optional): "))
        imdb_votes = int(input("Enter number of IMDb votes (optional): "))
        imdb_info["rating"] = imdb_rating
        imdb_info["votes"] = imdb_votes
    except ValueError:
        pass  # Ignore invalid input for optional fields
    movie["imdb"] = imdb_info

    tomatoes_info = {}
    tomatoes_production = input("Enter production company on Rotten Tomatoes (optional): ")
    if tomatoes_production:
        try:
            tomatoes_viewer_rating = float(input("Enter Rotten Tomatoes viewer rating (optional): "))
            tomatoes_viewer_reviews = int(input("Enter number of Rotten Tomatoes reviews (optional): "))
            tomatoes_meter = int(input("Enter Rotten Tomatoes meter score (optional): "))
            tomatoes_info["viewer"] = {
                "rating": tomatoes_viewer_rating,
                "numReviews": tomatoes_viewer_reviews,
                "meter": tomatoes_meter
            }
            tomatoes_info["production"] = tomatoes_production
        except ValueError:
            print("Invalid input for Rotten Tomatoes. Skipping...")
    movie["tomatoes"] = tomatoes_info

    movie["year"] = datetime.strptime(release_date_str, "%Y-%m-%d").date().year

    return movie

def create_user_document():
    """
    Prompts user for information and creates a user document.
    """
    MIN_PASSWORD_LENGTH = 8
    user = {}

    user["name"] = input("Enter user name: ")
    email = input("Enter email address: ")

    # Basic email validation (check for "@" and ".")
    if "@" not in email or "." not in email:
        print("Invalid email format. Please try again.")
        return None

    user["email"] = email.lower()  # Ensure consistent email format

    # Prompt for password with validation
    while True:
        password = input("Enter password (minimum {} characters): ".format(MIN_PASSWORD_LENGTH))
        password_confirm = input("Confirm password: ")

        if password != password_confirm:
            print("Passwords do not match. Please try again.")
            continue

        if len(password) < MIN_PASSWORD_LENGTH:
            print("Password is too short. Minimum {} characters required.".format(MIN_PASSWORD_LENGTH))
            continue

        # Hash the password with bcrypt
        user["password"] = bcrypt.hashpw(bytes(password,"UTF-8"),bcrypt.gensalt())
        break

    return user

def create_comment_document():
    """
    Prompts user for information and creates a comment document.
    """
    comment = {}

    comment["name"] = input("Enter your name (optional): ") or None  # Allow empty name
    comment["email"] = input("Enter your email (optional): ") or None  # Allow empty email

    movie_id_str = input("Enter the movie ID (ObjectId format): ")

    comment["movie_id"] = ObjectId(movie_id_str)

    comment["text"] = input("Enter your comment: ")
    comment["date"] = datetime.utcnow().isoformat()

    return comment

def create_theater_document():
    """
    Prompts user for information and creates a theater document.
    """
    theater = {}

    # Get theater ID (integer)
    while True:
        try:
            theater_id = int(input("Enter theater ID (integer): "))
            break
        except ValueError:
            print("Invalid theater ID. Please enter an integer.")

    theater["theaterId"] = theater_id

    # Get address information
    address = {}
    address["street1"] = input("Enter street address: ")
    address["city"] = input("Enter city: ")
    address["state"] = input("Enter state (abbreviation): ")
    address["zipcode"] = input("Enter zipcode: ")
    theater["location"] = {"address": address}

    # Get geographical coordinates (float)
    while True:
        try:
            longitude = float(input("Enter longitude (decimal degrees): "))
            latitude = float(input("Enter latitude (decimal degrees): "))
            break
        except ValueError:
            print("Invalid coordinates. Please enter decimal numbers.")

    theater["location"]["geo"] = {"type": "Point", "coordinates": [longitude, latitude]}

    return theater

class MongoInsertAPI:

    def __init__(self, mongo_client,  database_name):
        self.client = mongo_client
        self.db = self.client[database_name]
        self.comments = self.db["comments"]
        self.movies = self.db["movies"]
        self.theatres = self.db["theatres"]
        self.users = self.db["users"]

    def insert_comment(self):
        """
        Inserts a new comment document into the comments collection.

        Args:
            comment_data (dict): A dictionary containing the comment details.
                Expected keys: user_id, movie_id, content, timestamp (optional)

        Returns:
            ObjectId: The ObjectId of the inserted document or None if insertion fails.
        """
        comment_data = create_comment_document()
        try:
            result = self.comments.insert_one(comment_data)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting comment: {e}")
            return None

    def insert_movie(self):
        """
        Inserts a new movie document into the movies collection.

        Args:
            movie_data (dict): A dictionary containing the movie details.
                Expected keys: title, description, genre, release_date, cast (list of strings), director

        Returns:
            ObjectId: The ObjectId of the inserted document or None if insertion fails.
        """
        movie_data = create_movie_document()

        try:
            result = self.movies.insert_one(movie_data)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting movie: {e}")
            return None

    def insert_theatre(self):
        """
        Inserts a new theatre document into the theatres collection.

        Args:
            theatre_data (dict): A dictionary containing the theatre details.
                Expected keys: name, location, screens (int)

        Returns:
            ObjectId: The ObjectId of the inserted document or None if insertion fails.
        """
        theatre_data = create_theater_document()

        try:
            result = self.theatres.insert_one(theatre_data)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting theatre: {e}")
            return None

    def insert_user(self):
        """
        Inserts a new user document into the users collection.

        Args:
            user_data (dict): A dictionary containing the user details.
                Expected keys: username, email, password (hashed)

        Returns:
            ObjectId: The ObjectId of the inserted document or None if insertion fails.
        """
        user_data = create_user_document()

        try:
            result = self.users.insert_one(user_data)
            return result.inserted_id
        except Exception as e:
            print(f"Error inserting user: {e}")
            return None


if __name__ == "__main__":
    obj = MongoInsertAPI(connect(),"movieFlix")
    obj.insert_movie()
    obj.insert_user()
    obj.insert_comment()
    obj.insert_theatre()