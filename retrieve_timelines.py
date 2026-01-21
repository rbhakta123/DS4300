"""
DS 4300 HW 1 Extension
filename: retrieve_timelines.py
Timeline Retrieval Driver Program - Repeatedly retrieves home timelines for random users
Author: Ruhan Bhakta

Example output:
Starting timeline retrieval...
  Retrieved 100 timelines...
  Retrieved 200 timelines...
  ...
Successfully retrieved:  1000
Failed retrievals:       0
Empty timelines:         23
Time elapsed:            12.45 seconds
Retrieval rate:          80.32 timelines/second
Avg tweets per timeline: 9.77
Timeline calls/sec:      80.32
Database connection closed
"""

import time
from typing import Tuple, Optional
from twitter_api import TwitterAPI, Tweet


class TimelineRetriever:
    """Driver class for retrieving user home timelines from the database"""

    def __init__(self, db_api: TwitterAPI):
        self.db_api = db_api
        self.timelines_retrieved = 0
        self.timelines_failed = 0
        self.empty_timelines = 0
        self.total_tweets = 0

    def retrieve_timelines(
            self,
            num_retrievals: int
    ) -> Tuple[int, int, int, float]:
        """
        Retrieve home timelines for random users.

        Args:
            num_retrievals: Number of timelines to retrieve

        Returns:
            Tuple of (successful_retrievals, failed_retrievals, empty_timelines, elapsed_time)
        """
        print(f"Starting timeline retrieval for {num_retrievals} random users...")

        self.timelines_retrieved = 0
        self.timelines_failed = 0
        self.empty_timelines = 0
        self.total_tweets = 0

        if not self.db_api.is_connected():
            print("Error: Not connected to database")
            return 0, 0, 0, 0.0

        start_time = time.time()

        for i in range(1, num_retrievals + 1):
            # Get a random user
            user_id = self.db_api.get_random_user()

            if user_id is None:
                self.timelines_failed += 1
                continue

            # Retrieve their home timeline
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

        elapsed_time = time.time() - start_time
        self._print_results(elapsed_time)
        return self.timelines_retrieved, self.timelines_failed, self.empty_timelines, elapsed_time

    def retrieve_specific_user_timeline(self, user_id: int) -> None:
        """
        Retrieve and display the home timeline for a specific user.

        Args:
            user_id: The user ID to retrieve timeline for
        """
        print(f"\nRetrieving timeline for user {user_id}...")

        timeline = self.db_api.get_home_timeline(user_id)

        if timeline is None:
            print(f"  Error: Failed to retrieve timeline for user {user_id}")
            return

        if len(timeline) == 0:
            print(f"  User {user_id} has an empty timeline (follows no one or followees have no tweets)")
            return

        print(f"  Found {len(timeline)} tweets in timeline:")
        for idx, tweet in enumerate(timeline, 1):
            print(f"    {idx}. [{tweet.tweet_ts}] User {tweet.user_id}: {tweet.tweet_text[:60]}...")

    def _print_results(self, elapsed_time: float) -> None:
        """
        Print retrieval results and profiling stats
        """
        total = self.timelines_retrieved + self.timelines_failed

        print(f"\n{'=' * 50}")
        print(f"Successfully retrieved:  {self.timelines_retrieved}")
        print(f"Failed retrievals:       {self.timelines_failed}")
        print(f"Time elapsed:            {elapsed_time:.2f} seconds")

        if elapsed_time > 0 and self.timelines_retrieved > 0:
            # Print timeline profiling from API
            api_stats = self.db_api.get_profile_stats("timeline")
            print(f"Timeline calls/sec:      {api_stats['calls_per_sec']:.2f}")

        print(f"{'=' * 50}")


def main():
    """Main driver function"""
    DB_CONFIG = {
        "host": "localhost",
        "user": "ruhan",
        "password": "abc123",
        "database": "twitter",
        "autocommit": True,  # Read-only operations, autocommit is fine
    }

    # Configuration
    NUM_RETRIEVALS = 5000

    # Connect to database
    db_api = TwitterAPI(**DB_CONFIG)

    if not db_api.connect():
        print("Failed to connect to database")
        return

    try:
        retriever = TimelineRetriever(db_api)

        # Example: Retrieve timeline for a specific user
        # Uncomment the line below to see a detailed timeline for user 1
        # retriever.retrieve_specific_user_timeline(1)

        # Main retrieval loop
        retriever.retrieve_timelines(NUM_RETRIEVALS)

    except KeyboardInterrupt:
        print("\n\nRetrieval interrupted by user")
    finally:
        db_api.disconnect()


if __name__ == "__main__":
    main()