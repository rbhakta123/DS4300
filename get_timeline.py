"""
Test program for optimized timeline functionality
Uses non-JOIN approach for maximum speed
"""

import time
import random
from twitter_api import TwitterAPI


def main():
    """Test the non-JOIN timeline approach"""

    # Database configuration
    config = {
        'host': 'localhost',
        'user': 'ruhan',
        'password': 'abc123',
        'database': 'twitter'
    }

    api = TwitterAPI(**config)

    if not api.connect():
        print("Failed to connect")
        return

    try:
        # Preload users once
        print("Loading user IDs...")
        user_ids = api.get_all_follower_ids()
        if not user_ids:
            print("No users found")
            return
        print(f"Loaded {len(user_ids)} user IDs\n")

        iteration = 0
        start_time = time.time()
        last_report_time = start_time

        print("Starting timeline loading (non-JOIN approach)...\n")

        while True:
            user_id = random.choice(user_ids)
            api.get_timeline_optimized(user_id)  # Use optimized non-JOIN version
            iteration += 1

            current_time = time.time()
            if current_time - last_report_time >= 5.0:
                elapsed = current_time - start_time
                rate = iteration / elapsed
                print(f"  >>> Stats: {iteration:,} timelines in {elapsed:.2f}s = {rate:.0f} timelines/sec")
                last_report_time = current_time

    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        rate = iteration / elapsed if elapsed > 0 else 0
        print(f"\n\n=== Final Stats ===")
        print(f"Total timelines: {iteration:,}")
        print(f"Total time: {elapsed:.2f}s")
        print(f"Average rate: {rate:.0f} timelines/sec")

    finally:
        api.disconnect()


if __name__ == "__main__":
    main()