"""
DS 4300 HW 1
filename: twitter_api.py
API to interact with Redis Twitter database. Uses fanout-on-write with pipelining for efficiency.
Author: Ruhan Bhakta (Modified for Redis)
"""
import time
import json
import redis
from typing import Optional, List, Dict
from dataclasses import dataclass
from datetime import datetime
import random


@dataclass
class Tweet:
    """Data class representing a tweet"""
    tweet_id: int
    user_id: int
    tweet_ts: datetime
    tweet_text: str

    def __repr__(self):
        return f"Tweet(id={self.tweet_id}, user={self.user_id}, ts={self.tweet_ts}, text='{self.tweet_text[:30]}...')"


class TwitterAPI:
    """
    API for interacting with Twitter Redis database.

    Uses fanout-on-write strategy with pipelining:
    - Tweets are immediately pushed to all followers' home timelines
    - Uses Redis pipelining to batch follower updates efficiently
    - Result: Instant timeline reads (just ZREVRANGE + fetch tweets)
    """

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0,
                 password: Optional[str] = None, **kwargs):
        """
        Initialize Redis connection parameters.
        Note: autocommit parameter kept for compatibility but not used (Redis auto-commits)
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password

        self.redis_client = None

        # Profiling
        self.profile_call_count = 0
        self.profile_start_time = None
        self.timeline_call_count = 0

        # Tweet ID counter - stored in Redis
        self.tweet_counter_key = "tweet:counter"
        self.users_with_timelines_key = "users_with_timelines"

    def connect(self) -> bool:
        """Establish connection to Redis."""
        try:
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True  # Automatically decode responses to strings
            )

            # Test connection
            self.redis_client.ping()

            # Initialize tweet counter if it doesn't exist
            if not self.redis_client.exists(self.tweet_counter_key):
                self.redis_client.set(self.tweet_counter_key, 0)

            # Initialize profiling
            self.profile_start_time = time.time()
            self.profile_call_count = 0
            self.timeline_call_count = 0

            print(f"Successfully connected to Redis at {self.host}:{self.port}")
            return True

        except redis.ConnectionError as e:
            print(f"Error connecting to Redis: {e}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False

    def disconnect(self) -> None:
        """Close the Redis connection."""
        if self.redis_client:
            self.redis_client.close()
            print("Redis connection closed")

    def post_tweet(self, user_id: int, tweet_text: str) -> Optional[int]:
        """
        Insert a single tweet into Redis.

        Fanout-on-write strategy with denormalization:
        - Tweet stored as hash at "tweet:{tweet_id}" (for potential other queries)
        - Complete tweet data (JSON) added to ALL followers' home timelines
        - Result: Single-command timeline retrieval (no secondary lookups needed)
        """
        try:
            # Generate unique tweet ID
            tweet_id = self.redis_client.incr(self.tweet_counter_key)

            # Current timestamp (in seconds since epoch for sorting)
            tweet_ts = time.time()

            # Create pipeline for batch operations
            pipe = self.redis_client.pipeline(transaction=False)

            # Store tweet data as a hash (keeping for backwards compatibility/other queries)
            tweet_key = f"tweet:{tweet_id}"
            pipe.hset(tweet_key, mapping={
                'tweet_id': tweet_id,
                'user_id': user_id,
                'tweet_ts': tweet_ts,
                'tweet_text': tweet_text
            })

            # Prepare complete tweet data as JSON for denormalized storage
            tweet_data_json = json.dumps({
                'tweet_id': tweet_id,
                'user_id': user_id,
                'tweet_ts': tweet_ts,
                'tweet_text': tweet_text
            })

            # Add complete tweet data to all followers' home timelines
            followers_key = f"followers:{user_id}"
            followers = self.redis_client.smembers(followers_key)

            for follower_id in followers:
                home_timeline_key = f"home_timeline:{follower_id}"
                # Store complete tweet data (as JSON string) with timestamp as score
                pipe.zadd(home_timeline_key, {tweet_data_json: tweet_ts})

                # Trim timeline to keep only 10 most recent tweets
                # ZREMRANGEBYRANK removes elements by rank (0-indexed)
                # Keep elements from rank 0-9 (10 most recent), remove rest
                pipe.zremrangebyrank(home_timeline_key, 0, -11)

                # track users with timelines
                pipe.sadd(self.users_with_timelines_key, follower_id)

            # Execute all operations in one batch
            pipe.execute()

            self.profile_call_count += 1
            return tweet_id

        except Exception as e:
            print(f"Error posting tweet: {e}")
            return None

    def get_home_timeline(self, user_id: int) -> Optional[List[Tweet]]:
        """
        Retrieve the home timeline for a given user.
        Returns all tweets in the timeline (maximum 10 since timelines are trimmed).

        With denormalized storage: ULTRA FAST - single Redis command gets everything!
        No secondary lookups needed since complete tweet data is stored in the timeline.
        """
        try:
            home_timeline_key = f"home_timeline:{user_id}"

            # Get all tweet data (as JSON strings) sorted by timestamp, descending
            # Single command retrieves everything - no pipeline needed!
            tweet_data_list = self.redis_client.zrevrange(home_timeline_key, 0, -1)

            self.timeline_call_count += 1

            if not tweet_data_list:
                return []

            # Parse JSON and convert to Tweet objects
            tweets = []
            for tweet_json in tweet_data_list:
                try:
                    data = json.loads(tweet_json)
                    tweet = Tweet(
                        tweet_id=int(data['tweet_id']),
                        user_id=int(data['user_id']),
                        tweet_ts=datetime.fromtimestamp(data['tweet_ts']),
                        tweet_text=data['tweet_text']
                    )
                    tweets.append(tweet)
                except (json.JSONDecodeError, KeyError) as e:
                    # Skip malformed tweet data
                    print(f"Warning: Skipping malformed tweet data: {e}")
                    continue

            return tweets

        except Exception as e:
            print(f"Error retrieving timeline: {e}")
            return None

    def get_random_user(self) -> Optional[int]:
        try:
            user_id = self.redis_client.srandmember(self.users_with_timelines_key)
            return int(user_id) if user_id is not None else None
        except Exception as e:
            print(f"Error getting random user: {e}")
            return None

    def is_connected(self) -> bool:
        """Check if Redis connection is active."""
        try:
            if self.redis_client:
                self.redis_client.ping()
                return True
            return False
        except:
            return False

    def commit(self) -> bool:
        """
        Commit pending transactions.
        Note: Redis doesn't need explicit commits, kept for compatibility.
        """
        return True

    def get_profile_stats(self, call_type: str) -> dict:
        """Get profiling statistics for API calls."""
        if self.profile_start_time is None:
            return {"calls_per_sec": 0.0, "total_calls": 0, "elapsed_time": 0.0}

        elapsed = time.time() - self.profile_start_time
        if elapsed <= 0:
            return {"calls_per_sec": 0.0, "total_calls": 0, "elapsed_time": 0.0}

        if call_type == "timeline":
            calls = self.timeline_call_count
        elif call_type == "post_tweet":
            calls = self.profile_call_count
        else:
            raise ValueError("call_type must be 'timeline' or 'post_tweet'")

        return {
            "calls_per_sec": calls / elapsed,
            "total_calls": calls,
            "elapsed_time": elapsed,
        }

    def load_follows_from_csv(self, filename: str) -> int:
        """
        Load the follows relationships from CSV file.
        CSV format: follower_id, followee_id

        Uses pipelining for efficient batch loading.
        Storage: For each user, maintain a set of followers at key "followers:{followee_id}"
        """
        import csv

        follows_loaded = 0

        try:
            print(f"Loading follows relationships from: {filename}")

            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                # Skip header if present
                next(reader, None)

                # Use pipeline for batch operations
                pipe = self.redis_client.pipeline(transaction=False)
                batch_size = 5000

                for follower_id, followee_id in reader:
                    follower_id = int(follower_id)
                    followee_id = int(followee_id)

                    # Add follower to followee's followers set
                    followers_key = f"followers:{followee_id}"
                    pipe.sadd(followers_key, follower_id)

                    follows_loaded += 1

                    # Execute pipeline every batch_size operations
                    if follows_loaded % batch_size == 0:
                        pipe.execute()
                        pipe = self.redis_client.pipeline(transaction=False)

                        if follows_loaded % 50000 == 0:
                            print(f"  Loaded {follows_loaded} follows relationships...")

                # Execute any remaining operations
                if follows_loaded % batch_size != 0:
                    pipe.execute()

            print(f"Successfully loaded {follows_loaded} follows relationships")
            return follows_loaded

        except Exception as e:
            print(f"Error loading follows: {e}")
            return follows_loaded