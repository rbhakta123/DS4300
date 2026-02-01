"""
DS 4300 HW 1
filename: retrieve_timelines.py
Twitter Timeline retrieval Driver - Retrieves home timelines for specified number of random users.
Uses twitter_api.py to interact with Redis.
Author: Ruhan Bhakta

Stats:
Successfully retrieved:  10000
Failed retrievals:       0

Time elapsed:            11.46 seconds
getTimeline calls/sec:   872.67
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
    """Driver class for retrieving user home timelines from Redis"""

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
            print("Error: Not connected to Redis")
            return 0, 0, 0, 0.0

        try:
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

        except KeyboardInterrupt:
            print("\n\n⚠️  Retrieval interrupted by user!")
            print(f"Retrieved {self.timelines_retrieved} timelines before interrupt")
        finally:
            # Always print results, even if interrupted
            self._print_results()
        return self.timelines_retrieved, self.timelines_failed, self.empty_timelines, self.db_api.get_profile_stats("timeline")['elapsed_time']

    def _print_results(self) -> None:
        """Print retrieval results and profiling stats"""
        total = self.timelines_retrieved + self.timelines_failed

        print(f"\n{'=' * 60}")
        print("TIMELINE RETRIEVAL RESULTS")
        print("=" * 60)
        print(f"Successfully retrieved:  {self.timelines_retrieved}")
        print(f"Failed retrievals:       {self.timelines_failed}")

        # Print profiling stats
        api_stats = self.db_api.get_profile_stats("timeline")
        print(f"\nTime elapsed:            {api_stats['elapsed_time']:.2f} seconds")
        if self.timelines_retrieved > 0:
            print(f"getTimeline calls/sec:   {api_stats['calls_per_sec']:.2f}")
        print("=" * 60)


def main():
    """Main driver function"""
    # Redis configuration
    REDIS_CONFIG = {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", 6379)),
        "db": int(os.getenv("REDIS_DB", 0)),
        "password": os.getenv("REDIS_PASSWORD"),  # None if not set
    }

    NUM_RETRIEVALS = 10000

    # Connect to Redis
    db_api = TwitterAPI(**REDIS_CONFIG)
    if not db_api.connect():
        print("Failed to connect to Redis")
        return

    try:
        retriever = TimelineRetriever(db_api)
        # Main retrieval loop
        retriever.retrieve_timelines(NUM_RETRIEVALS)
    except KeyboardInterrupt:
        print("\n\n⚠️  Program interrupted by user")
    finally:
        db_api.disconnect()


if __name__ == "__main__":
    main()