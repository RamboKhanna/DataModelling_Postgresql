import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open song file
    df = pd.read_json(path_or_buf=filepath,lines=True)

    # insert song record
    song_data = [list(row) for row in df[["song_id","title","artist_id","year","duration"]].itertuples(index=False)]
    for item in song_data:
        cur.execute(song_table_insert, item)
    
    # insert artist record
    artist_data = [list(row) for row in
                   df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].itertuples(index=False)]
    for item in artist_data:
        cur.execute(artist_table_insert, item)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the time information in order to store it into the time table.
    Then it extracts the user information in order to store it into the users table.
    Then it extracts the songplay information in order to store it into the songplays table.
    
    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    
    # open log file
    df=pd.read_json(path_or_buf=filepath,lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"].reset_index(drop=True).copy(deep=True)

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts']).reset_index(drop=True).to_frame()
    t['hour']=t['ts'].dt.strftime('%-H') #int
    t['day']=t['ts'].dt.strftime('%-d') #int
    t['week']=t['ts'].dt.strftime('%W') #int
    t['month']=t['ts'].dt.strftime('%B') #varchar
    t['year']=t['ts'].dt.strftime('%Y') #int
    t['weekday']=t['ts'].dt.strftime('%A') #varchar
    
    # insert time data records
    time_df = t.copy(deep=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]].copy(deep=True)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    #create column start_time
    df['start_time'] = pd.to_datetime(df['ts']).reset_index(drop=True).to_frame()
    
    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.start_time,row.userId,row.level,
                         songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This prcedure opens the files one by one and inputs data into the created tables using the above functions
    
    INPUTS: 
    * cur the cursor variable
    * conn created connection to db
    * filepath the file path to the data directory
    * func respective function process_log_file/process_song_file
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main loop of the program.
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    #conn.set_session(autocommit=True)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()