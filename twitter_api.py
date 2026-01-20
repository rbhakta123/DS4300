"""
DS 4300 HW 1
filename: load_tweets.py
Twitter Database API- Interacts with DB to make driver functions DB agnostic
author: Ruhan Bhakta
"""
import time
import mysql.connector
from mysql.connector import Error
from typing import Optional

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

        # Profiling
        self.profile_call_count = 0
        self.profile_start_time = None

        # Prepare query
        self._insert_tweet_sql = (
            "INSERT INTO TWEET (user_id, tweet_text) VALUES (%s, %s)"
        )

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

            # Reuse a single cursor for all inserts
            self.cursor = self.connection.cursor()

            # Start profiling
            self.profile_start_time = time.time()
            self.profile_call_count = 0

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

    def is_connected(self) -> bool:
        """Check if database connection is active."""
        return (
            self.connection is not None
            and self.connection.is_connected()
        )

    def commit(self) -> bool:
        """
        Commit pending transactions.
        """
        try:
            if self.connection and not self.autocommit:
                self.connection.commit()
            return True
        except Error:
            return False

    def get_profile_stats(self) -> dict:
        """
        Get profiling statistics for post_tweet calls.
        """
        if self.profile_start_time is None:
            return {"calls_per_sec": 0.0, "total_calls": 0}

        elapsed = time.time() - self.profile_start_time
        calls_per_sec = self.profile_call_count / elapsed if elapsed > 0 else 0.0

        return {
            "calls_per_sec": calls_per_sec,
            "total_calls": self.profile_call_count
        }