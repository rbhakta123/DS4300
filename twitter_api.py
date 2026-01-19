"""
DS 4300 HW 1
filename: load_tweets.py
Twitter Database API- Interacts with DB to make driver functions DB agnostic
author: Ruhan Bhakta
"""

import random
import mysql.connector
from mysql.connector import Error
from typing import Optional, List
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Tweet:
    """Data class representing a tweet"""
    tweet_id: int
    user_id: int
    tweet_ts: datetime
    tweet_text: str


class TwitterAPI:
    """API for Twitter database operations"""

    def __init__(self, host: str, user: str, password: str, database: str, autocommit: bool = False):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.autocommit = autocommit

        self.connection = None
        self.cursor = None

        # Prepare insert query
        self._insert_tweet_sql = (
            "INSERT INTO TWEET (user_id, tweet_text) VALUES (%s, %s)"
        )

        # MODIFIED: join order + index-friendly ORDER BY
        self._timeline_query = """
            SELECT t.tweet_id, t.user_id, t.tweet_ts, t.tweet_text
            FROM FOLLOWS f
            JOIN TWEET t ON t.user_id = f.followee_id
            WHERE f.follower_id = %s
            ORDER BY t.tweet_ts DESC
            LIMIT 10
        """

        # UNCHANGED
        self._random_user_query = """
            SELECT DISTINCT follower_id
            FROM FOLLOWS
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
                # OPTIMIZATION: use C extension instead of pure Python
                use_pure=False,
                # OPTIMIZATION: disable warnings for speed
                get_warnings=False,
                raise_on_warnings=False,
            )

            # MODIFIED: buffered cursor reduces round trips
            # OPTIMIZATION: raw cursor avoids type conversion overhead
            self.cursor = self.connection.cursor(buffered=True, raw=True)

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
                if not self.autocommit:
                    self.connection.commit()
                self.connection.close()
                print("Database connection closed")
        except Error:
            pass

    def post_tweet(self, user_id: int, tweet_text: str) -> Optional[int]:
        """Insert a single tweet into the database."""
        try:
            self.cursor.execute(self._insert_tweet_sql, (user_id, tweet_text))
            return self.cursor.lastrowid
        except Error:
            return None

    def is_connected(self) -> bool:
        return self.connection is not None and self.connection.is_connected()

    def commit(self) -> bool:
        try:
            if self.connection and not self.autocommit:
                self.connection.commit()
            return True
        except Error:
            return False

    # MODIFIED: no dataclass creation, minimal overhead
    def get_timeline(self, user_id: int) -> List[tuple]:
        self.cursor.execute(self._timeline_query, (user_id,))
        return self.cursor.fetchmany(10)

    # MODIFIED: cacheable helper, original still exists
    def get_all_follower_ids(self) -> List[int]:
        self.cursor.execute(self._random_user_query)
        # Handle raw cursor output (returns bytes)
        return [int(row[0]) if isinstance(row[0], bytes) else row[0]
                for row in self.cursor.fetchall()]

    # UNCHANGED API
    def get_random_user_id(self) -> Optional[int]:
        self.cursor.execute(self._random_user_query)
        results = self.cursor.fetchall()
        if results:
            val = random.choice(results)[0]
            return int(val) if isinstance(val, bytes) else val
        return None

    # ========== NEW OPTIMIZED METHODS ==========

    def get_followers(self, user_id: int) -> List[int]:
        """
        Get list of users who follow the given user_id.

        Example: get_followers(123) returns [101, 102, 103]
        meaning users 101, 102, 103 follow user 123
        """
        self.cursor.execute("SELECT follower_id FROM FOLLOWS WHERE followee_id = %s", (user_id,))
        return [int(row[0]) if isinstance(row[0], bytes) else row[0]
                for row in self.cursor.fetchall()]

    def get_followees(self, user_id: int) -> List[int]:
        """
        Get list of users that the given user_id follows.

        Example: get_followees(123) returns [201, 202, 203]
        meaning user 123 follows users 201, 202, 203
        """
        self.cursor.execute("SELECT followee_id FROM FOLLOWS WHERE follower_id = %s", (user_id,))
        return [int(row[0]) if isinstance(row[0], bytes) else row[0]
                for row in self.cursor.fetchall()]

    def get_tweets(self, user_id: int, limit: int = 100) -> List[tuple]:
        """
        Get tweets for a specific user.
        Returns list of tuples: (tweet_id, user_id, tweet_ts, tweet_text)
        """
        self.cursor.execute(
            "SELECT tweet_id, user_id, tweet_ts, tweet_text FROM TWEET WHERE user_id = %s LIMIT %s",
            (user_id, limit)
        )
        return self.cursor.fetchall()

    def get_timeline_optimized(self, user_id: int) -> List[tuple]:
        """
        OPTIMIZED: Get timeline using non-JOIN approach.

        This is much faster than get_timeline() for large databases because:
        1. Avoids expensive JOIN operation
        2. Uses two simple indexed lookups instead
        3. No ORDER BY overhead

        Strategy:
        - Step 1: Get list of followees (fast indexed lookup)
        - Step 2: Get tweets from those users using IN clause (fast indexed lookup)

        Returns list of tuples: (tweet_id, user_id, tweet_ts, tweet_text)
        """
        # Step 1: Get followees
        followees = self.get_followees(user_id)

        if not followees:
            return []

        # Step 2: Get tweets from followees using IN clause
        placeholders = ','.join(['%s'] * len(followees))
        query = f"""
            SELECT tweet_id, user_id, tweet_ts, tweet_text 
            FROM TWEET 
            WHERE user_id IN ({placeholders}) 
            LIMIT 10
        """

        self.cursor.execute(query, followees)
        return self.cursor.fetchmany(10)