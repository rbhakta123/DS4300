"""
DS 4300 HW 1 Extension
filename: twitter_api_extended.py
Extended Twitter Database API with timeline retrieval functionality
Author: Ruhan Bhakta
"""
import time
import mysql.connector
from mysql.connector import Error
from typing import Optional, List, Dict
from dataclasses import dataclass
from datetime import datetime

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
    """API for interacting with Twitter MySQL database"""

    def __init__(self, host: str, user: str, password: str, database: str, autocommit: bool = False):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.autocommit = autocommit
        # For quicker picking of random users, store min and max user ids
        self.min_user_id = None
        self.max_user_id = None
        self.connection = None
        self.cursor = None
        # Profiling
        self.profile_call_count = 0
        self.profile_start_time = None
        self.timeline_call_count = 0

        # Prepared queries
        self._insert_tweet_sql = (
            "INSERT INTO TWEET (user_id, tweet_text) VALUES (%s, %s)"
        )

        self._get_timeline_sql = """
                                 SELECT t.tweet_id, t.user_id, t.tweet_ts, t.tweet_text
                                 FROM TWEET t
                                          INNER JOIN FOLLOWS f ON t.user_id = f.followee_id
                                 WHERE f.follower_id = %s
                                 ORDER BY t.tweet_ts DESC LIMIT 10 \
                                 """

        # Query to get a random user
        self._get_random_user_sql = """
                                    SELECT DISTINCT user_id
                                    FROM TWEET
                                    WHERE user_id >= FLOOR(
                                            RAND() * (%s - %s + 1) + %s
                                                     )
                                    ORDER BY user_id LIMIT 1 \
                                    """

    def connect(self) -> bool:
        """Establish connection to the database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=self.autocommit,
            )
            self.cursor = self.connection.cursor()

            # Cache user_id bounds for quicker random id generation
            self.cursor.execute(
                "SELECT MIN(user_id), MAX(user_id) FROM TWEET"
            )
            row = self.cursor.fetchone()
            self.min_user_id = row[0]
            self.max_user_id = row[1]

            # profiling
            self.profile_start_time = time.time()
            self.profile_call_count = 0
            self.timeline_call_count = 0

            print(f"Successfully connected to database '{self.database}'")
            return True

        except Error as e:
            print(f"Error connecting to database: {e}")
            return False

    def disconnect(self) -> None:
        """Close the database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection and self.connection.is_connected():
                # Final commit if autocommit is off
                if not self.autocommit:
                    self.connection.commit()
                self.connection.close()
                print("Database connection closed")
        except Error:
            pass

    def post_tweet(self, user_id: int, tweet_text: str) -> Optional[int]:
        """
        Insert a single tweet into the database.
        """
        try:
            self.cursor.execute(
                self._insert_tweet_sql, (user_id, tweet_text)
            )
            self.profile_call_count += 1
            return self.cursor.lastrowid

        except Error:
            return None

    def get_home_timeline(self, user_id: int) -> Optional[List[Tweet]]:
        """Retrieve the home timeline for a given user. Returns a list of Tweet objects."""
        try:
            self.cursor.execute(self._get_timeline_sql, (user_id,))
            rows = self.cursor.fetchall()

            self.timeline_call_count += 1

            # Convert rows to Tweet objects
            tweets = []
            for row in rows:
                tweet = Tweet(
                    tweet_id=row[0],
                    user_id=row[1],
                    tweet_ts=row[2],
                    tweet_text=row[3]
                )
                tweets.append(tweet)

            return tweets

        except Error as e:
            print(f"Error retrieving timeline: {e}")
            return None

    def get_random_user(self) -> Optional[int]:
        """
        Get a random user ID from the Tweet table"""
        if self.min_user_id is None or self.max_user_id is None:
            return None

        try:
            self.cursor.execute(
                self._get_random_user_sql,
                (
                    self.max_user_id,
                    self.min_user_id,
                    self.min_user_id
                )
            )
            row = self.cursor.fetchone()
            return row[0] if row else None

        except Error as e:
            print(f"Error getting random user: {e}")
            return None

    def is_connected(self) -> bool:
        """Check if database connection is active."""
        return (
            self.connection is not None
            and self.connection.is_connected()
        )

    def commit(self) -> bool:
        """Commit pending transactions. """
        try:
            if self.connection and not self.autocommit:
                self.connection.commit()
            return True
        except Error:
            return False

    def get_profile_stats(self, call_type: str) -> dict:
        """Get profiling statistics for API calls"""
        if self.profile_start_time is None:
            return {"calls_per_sec": 0.0, "total_calls": 0}

        elapsed = time.time() - self.profile_start_time
        if elapsed <= 0:
            return {"calls_per_sec": 0.0, "total_calls": 0}

        if call_type == "timeline":
            calls = self.timeline_call_count
        elif call_type == "post_tweet":
            calls = self.profile_call_count
        else:
            raise ValueError("call_type must be 'timeline' or 'post_tweet'")

        return {
            "calls_per_sec": calls / elapsed,
            "total_calls": calls,
        }
