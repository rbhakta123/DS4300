"""
DS 4300 HW 1
filename: load_tweets.py
Tweet Loader Driver Program- Reads tweets from a CSV file and inserts them into the database
Author: Ruhan Bhakta

Successfully loaded:    1000000
Failed to load:         0
Time elapsed:           480.06 seconds
Loading rate:           2083.08 tweets/second
"""

import csv
import time
from typing import Tuple
from twitter_api import TwitterAPI

class TweetLoader:
    """Driver class for loading tweets into a database table"""

    def __init__(self, db_api: TwitterAPI):
        self.db_api = db_api
        self.tweets_loaded = 0
        self.tweets_failed = 0

    def read_tweets_from_csv(self, filename: str):
        """
        Reads in tweets from a CSV file.
        """
        with open(filename, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            # skip header row
            next(reader)

            for user_id, tweet_text in reader:
                yield int(user_id), tweet_text

    def load_tweets(self, filename: str) -> Tuple[int, int, float]:
        """
        Load tweets into database one at a time.
        """
        print(f"Starting tweet loading from: {filename}")

        self.tweets_loaded = 0
        self.tweets_failed = 0
        if not self.db_api.is_connected():
            print("Error: Not connected to database")
            return 0, 0, 0.0
        start_time = time.time()
        print("Inserting tweets into database")
        for i, (user_id, tweet_text) in enumerate(self.read_tweets_from_csv(filename), 1):
            result = self.db_api.post_tweet(user_id, tweet_text)
            if result is not None:
                self.tweets_loaded += 1
            else:
                self.tweets_failed += 1
            if i % 10000 == 0:
                print(f"  Processed {i} tweets...")

        # Commit at the end
        self.db_api.commit()

        elapsed_time = time.time() - start_time
        self._print_results(elapsed_time)
        return self.tweets_loaded, self.tweets_failed, elapsed_time

    def _print_results(self, elapsed_time: float) -> None:
        """
        Print loading results.
        """
        total = self.tweets_loaded + self.tweets_failed

        print(f"Successfully loaded:    {self.tweets_loaded}")
        print(f"Failed to load:         {self.tweets_failed}")
        print(f"Time elapsed:           {elapsed_time:.2f} seconds")

        if elapsed_time > 0:
            rate = self.tweets_loaded / elapsed_time
            print(f"Loading rate:           {rate:.2f} tweets/second")
            print(f"Average time per tweet: {(elapsed_time / total * 1000):.2f} ms")

def main():
    DB_CONFIG = {
        "host": "localhost",
        "user": "ruhan",
        "password": "abc123",
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