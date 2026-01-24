"""
DS 4300 HW 1
filename: load_tweets.py
Tweet Loader Driver Program- Reads tweets from a CSV file and inserts them into the database. Uses twitter_api.py to
interact with the database
Author: Ruhan Bhakta

Stats:
Successfully loaded:    1000000
Time elapsed:           131.27 seconds
post_tweet calls/sec:   7617.62
"""
import os
import csv
import time
from typing import Tuple
from twitter_api import TwitterAPI

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class TweetLoader:
    """Driver class for loading tweets into a database table"""

    def __init__(self, db_api: TwitterAPI):
        self.db_api = db_api
        self.tweets_loaded = 0
        self.tweets_failed = 0

    def read_tweets_from_csv(self, filename: str):
        """Reads in tweets from a CSV file"""
        with open(filename, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            # skip header row
            next(reader)

            for user_id, tweet_text in reader:
                yield int(user_id), tweet_text

    def load_tweets(self, filename: str) -> Tuple[int, int, float]:
        """Load tweets into database one at a time"""
        print(f"Starting tweet loading from: {filename}")

        self.tweets_loaded = 0
        self.tweets_failed = 0
        if not self.db_api.is_connected():
            print("Error: Not connected to database")
            return 0, 0, 0.0
        print("Inserting tweets into database")
        for i, (user_id, tweet_text) in enumerate(self.read_tweets_from_csv(filename), 1):
            result = self.db_api.post_tweet(user_id, tweet_text)
            if result is not None:
                self.tweets_loaded += 1
            else:
                self.tweets_failed += 1
            # Report updates for every 10,000 tweets loaded
            if i % 10000 == 0:
                print(f"  Processed {i} tweets...")

        self._print_results()
        return self.tweets_loaded, self.tweets_failed, self.db_api.get_profile_stats("post_tweet")['elapsed_time']

    def _print_results(self) -> None:
        """Print loading results, and profiling stats"""

        print(f"Successfully loaded:    {self.tweets_loaded}")
        print(f"Failed to load:         {self.tweets_failed}")

        # Print profiling from API
        api_stats = self.db_api.get_profile_stats("post_tweet")
        print(f"Time elapsed: {api_stats['elapsed_time']:.2f} seconds")
        print(f"post_tweet calls/sec: {api_stats['calls_per_sec']:.2f}")

def main():
    DB_CONFIG = {
        "host": "localhost",
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": "twitter",
        "autocommit": False,
    }
    CSV_FILE = "hw1_data/tweet.csv"
    db_api = TwitterAPI(**DB_CONFIG)

    if not db_api.connect():
        print("Failed to connect to database")
        return
    try:
        loader = TweetLoader(db_api)
        loader.load_tweets(CSV_FILE)
    except KeyboardInterrupt:
        print("\nLoading interrupted by user")
    finally:
        db_api.disconnect()

if __name__ == "__main__":
    main()