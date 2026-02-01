"""
DS 4300 HW 1
filename: load_tweets.py
Tweet Loader Driver Program - Reads tweets from a CSV file and inserts them into Redis.
Uses twitter_api.py to interact with Redis.
Author: Ruhan Bhakta

Stats:
Successfully loaded:    175173
Failed to load:         0

Time elapsed:           201.76 seconds
post_tweet calls/sec:   868.24
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
    """Driver class for loading tweets into Redis"""

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
        """Load tweets into Redis one at a time"""
        print(f"Starting tweet loading from: {filename}")

        self.tweets_loaded = 0
        self.tweets_failed = 0

        if not self.db_api.is_connected():
            print("Error: Not connected to Redis")
            return 0, 0, 0.0

        print("Inserting tweets into Redis")

        try:
            for i, (user_id, tweet_text) in enumerate(self.read_tweets_from_csv(filename), 1):
                result = self.db_api.post_tweet(user_id, tweet_text)
                if result is not None:
                    self.tweets_loaded += 1
                else:
                    self.tweets_failed += 1

                # Report updates for every 10,000 tweets loaded
                if i % 10000 == 0:
                    print(f"  Processed {i} tweets...")
        except KeyboardInterrupt:
            print("\n\n⚠️  Loading interrupted by user!")
            print(f"Processed {self.tweets_loaded + self.tweets_failed} tweets before interrupt")
        finally:
            # Always print results, even if interrupted
            self._print_results()

        return self.tweets_loaded, self.tweets_failed, self.db_api.get_profile_stats("post_tweet")['elapsed_time']

    def _print_results(self) -> None:
        """Print loading results, and profiling stats"""

        print("\n" + "=" * 60)
        print("TWEET LOADING RESULTS")
        print("=" * 60)
        print(f"Successfully loaded:    {self.tweets_loaded}")
        print(f"Failed to load:         {self.tweets_failed}")

        # Print profiling from API
        api_stats = self.db_api.get_profile_stats("post_tweet")
        print(f"\nTime elapsed:           {api_stats['elapsed_time']:.2f} seconds")
        print(f"post_tweet calls/sec:   {api_stats['calls_per_sec']:.2f}")
        print("=" * 60)


def main():
    # Redis configuration
    REDIS_CONFIG = {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", 6379)),
        "db": int(os.getenv("REDIS_DB", 0)),
        "password": os.getenv("REDIS_PASSWORD"),  # None if not set
    }

    CSV_FILE = "hw1_data/tweet.csv"
    FOLLOWS_FILE = "hw1_data/follows.csv"

    # Initialize API
    db_api = TwitterAPI(**REDIS_CONFIG)

    if not db_api.connect():
        print("Failed to connect to Redis")
        return

    try:
        # First, load the follows relationships
        print("\n" + "=" * 60)
        print("PHASE 1: Loading Follows Relationships")
        print("=" * 60)
        db_api.load_follows_from_csv(FOLLOWS_FILE)

        # Then load the tweets
        print("\n" + "=" * 60)
        print("PHASE 2: Loading Tweets")
        print("=" * 60)
        loader = TweetLoader(db_api)
        loader.load_tweets(CSV_FILE)

    except KeyboardInterrupt:
        print("\n\n⚠️  Program interrupted by user")
    except FileNotFoundError as e:
        print(f"\n❌ Error: Could not find file - {e}")
        print("Please ensure both tweet.csv and follows.csv are in the hw1_data directory")
    finally:
        db_api.disconnect()


if __name__ == "__main__":
    main()