#!/usr/bin/env python3
"""
Script to create an SQLite database from CSV files with proper data type handling.
Creates tables for receiving stats and roster data from Mercyhurst University football team.
"""

import pandas as pd
import sqlite3
import numpy as np
from pathlib import Path

def create_football_database():
    """Create SQLite database with receiving and roster tables from CSV files."""
    
    # Database and CSV file paths
    db_path = "football_data.db"
    receiving_csv = "Data/receiving.csv"
    roster_csv = "Data/roster.csv"
    
    # Check if CSV files exist
    if not Path(receiving_csv).exists():
        raise FileNotFoundError(f"CSV file not found: {receiving_csv}")
    if not Path(roster_csv).exists():
        raise FileNotFoundError(f"CSV file not found: {roster_csv}")
    
    print("Reading CSV files...")
    
    # Read receiving data with proper data types
    print("Processing receiving.csv...")
    receiving_df = pd.read_csv(receiving_csv)
    
    # Handle the receiving data types
    # Note: AVG column might have NaN values for players with 0 receptions
    receiving_df['#'] = receiving_df['#'].astype('int64')
    receiving_df['GP'] = receiving_df['GP'].astype('int64')
    receiving_df['NO'] = receiving_df['NO'].astype('int64')
    receiving_df['YDS'] = receiving_df['YDS'].astype('int64')
    # AVG might have NaN values, so we'll keep it as float64 (default)
    receiving_df['TD'] = receiving_df['TD'].astype('int64')
    receiving_df['Long'] = receiving_df['Long'].astype('int64')
    # AVG/G might also have NaN/float values
    receiving_df['PlayerID'] = receiving_df['PlayerID'].astype('int64')
    
    print(f"Receiving data shape: {receiving_df.shape}")
    print("Receiving data types:")
    print(receiving_df.dtypes)
    print()
    
    # Read roster data with proper data types
    print("Processing roster.csv...")
    roster_df = pd.read_csv(roster_csv)
    
    # Handle roster data types
    roster_df['id'] = roster_df['id'].astype('int64')
    roster_df['number'] = roster_df['number'].astype('int64')
    # height stays as string (e.g., "6-4")
    # weight column has some NaN values, so we'll keep it as float64 to preserve them
    # Other columns (name, academic_class, hometown, photo_url, profile_url) stay as strings
    
    print(f"Roster data shape: {roster_df.shape}")
    print("Roster data types:")
    print(roster_df.dtypes)
    print()
    
    # Create SQLite database connection
    print(f"Creating SQLite database: {db_path}")
    conn = sqlite3.connect(db_path)
    
    try:
        # Create receiving table
        print("Creating receiving table...")
        receiving_df.to_sql('receiving', conn, if_exists='replace', index=False)
        
        # Create roster table
        print("Creating roster table...")
        roster_df.to_sql('roster', conn, if_exists='replace', index=False)
        
        # Verify the tables were created
        cursor = conn.cursor()
        
        # Check receiving table
        cursor.execute("SELECT COUNT(*) FROM receiving")
        receiving_count = cursor.fetchone()[0]
        print(f"Receiving table created with {receiving_count} records")
        
        # Check roster table
        cursor.execute("SELECT COUNT(*) FROM roster")
        roster_count = cursor.fetchone()[0]
        print(f"Roster table created with {roster_count} records")
        
        # Show table schemas
        print("\nReceiving table schema:")
        cursor.execute("PRAGMA table_info(receiving)")
        for row in cursor.fetchall():
            print(f"  {row}")
        
        print("\nRoster table schema:")
        cursor.execute("PRAGMA table_info(roster)")
        for row in cursor.fetchall():
            print(f"  {row}")
        
        # Show sample data
        print("\nSample receiving data:")
        cursor.execute("SELECT * FROM receiving LIMIT 3")
        for row in cursor.fetchall():
            print(f"  {row}")
        
        print("\nSample roster data:")
        cursor.execute("SELECT * FROM roster LIMIT 3")
        for row in cursor.fetchall():
            print(f"  {row}")
        
        print(f"\nDatabase successfully created: {db_path}")
        
    except Exception as e:
        print(f"Error creating database: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_football_database()