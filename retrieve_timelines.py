"""
DS 4300 HW 1
filename: retrieve_timelines.py
Twitter Timeline retrieval Driver- Retrieves home timelines for specified number of random users. Uses twitter_api.py to
interact with the database
Author: Ruhan Bhakta

Stats:
Successfully retrieved:  10000
Failed retrievals:       0
Time elapsed:            15.56 seconds
retrieve_timelines calls/sec:      642.68
"""
import os
from typing import Tuple
from twitter_api import TwitterAPI

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class TimelineRetriever:
    """Driver class for retrieving user home timelines from the database"""

    def __init__(self, db_api: TwitterAPI):
        self.db_api = db_api
        self.timelines_retrieved = 0
        self.timelines_failed = 0
        self.empty_timelines = 0
        self.total_tweets = 0

    def retrieve_timelines(self, num_retrievals: int) -> Tuple[int, int, int, float]:
        """
        Retrieve home timelines for random users. Returns a tuple of
        (successful_retrievals, failed_retrievals, empty_timelines, elapsed_time)
        """
        print(f"Starting timeline retrieval for {num_retrievals} random users")

        self.timelines_retrieved = 0
        self.timelines_failed = 0
        self.empty_timelines = 0
        self.total_tweets = 0

        if not self.db_api.is_connected():
            print("Error: Not connected to database")
            return 0, 0, 0, 0.0

        for i in range(1, num_retrievals + 1):
            # Get a random user
            user_id = self.db_api.get_random_user()
            if user_id is None:
                self.timelines_failed += 1
                continue
            # Retrieve their home timeline using api call
            timeline = self.db_api.get_home_timeline(user_id)

            if timeline is None:
                self.timelines_failed += 1
            else:
                self.timelines_retrieved += 1
                if len(timeline) == 0:
                    self.empty_timelines += 1
                else:
                    self.total_tweets += len(timeline)

            # Report progress every 100 retrievals
            if i % 100 == 0:
                print(f"  Retrieved {i} timelines...")
        self._print_results()
        return self.timelines_retrieved, self.timelines_failed, self.empty_timelines, self.db_api.get_profile_stats("timeline")['elapsed_time']

    def _print_results(self) -> None:
        """ Print retrieval results and profiling stats"""
        total = self.timelines_retrieved + self.timelines_failed

        print(f"\n{'=' * 50}")
        print(f"Successfully retrieved:  {self.timelines_retrieved}")
        print(f"Failed retrievals:       {self.timelines_failed}")

        # Print profiling stats
        api_stats = self.db_api.get_profile_stats("timeline")
        print(f"Time elapsed: {api_stats['elapsed_time']:.2f} seconds")
        if self.timelines_retrieved > 0:
            print(f"retrieve_timeline calls/sec: {api_stats['calls_per_sec']:.2f}")

def main():
    """Main driver function"""
    DB_CONFIG = {
        "host": "localhost",
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": "twitter",
        "autocommit": True
    }
    NUM_RETRIEVALS = 10000

    # Connect to database
    db_api = TwitterAPI(**DB_CONFIG)
    if not db_api.connect():
        print("Failed to connect to database")
        return
    try:
        retriever = TimelineRetriever(db_api)
        # Main retrieval loop
        retriever.retrieve_timelines(NUM_RETRIEVALS)
    except KeyboardInterrupt:
        print("\n\nRetrieval interrupted by user")
    finally:
        db_api.disconnect()

if __name__ == "__main__":
    main()