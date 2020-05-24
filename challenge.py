# Importing Dependencies
import json
import pandas as pd
import numpy as np
import re
import time
from sqlalchemy import create_engine
from config import sql_pw

# Extracting Data

file_dir = "C:/Users/Eric/Documents/GitHub/Movies-ETL/"

try:
    # Reading JSON from Wikipedia
    wiki_movies_raw = json.load(open(f'{file_dir}wikipedia.movies.json', mode='r'))
    print("Successfully extracted Wikipedia data")
except:
    print("Failed to extracted Wikipedia data")

try:
    # Read csv datasets from kaggle.com
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv')
    print("Successfully extracted Kaggle data")
except:
    print("Failed to extracted Kaggle data")

# Trasforming Data

# Cleaning Wikipedia Data

try:
    # Removing rows that do not have to do with movies
    wiki_movies = [movie for movie in wiki_movies_raw 
                if ("Director" in movie or "Directed by" in movie)
                    and "imdb_link" in movie
                    and "No. of episodes" not in movie]

    # Function for dropping alt_titles and merging similar columns together
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
        
        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
        
        return movie
    # Calling function and applying it to DataFrame
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)

    # Dropping imdb_id that doesn't match format
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    # Removing columns with majority null values
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns 
                            if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
    print("Successfully cleaned Wikipedia columns")
except:
    print("Failed to clean Wikipedia columns")

# Cleaning box office data

try:
    # Drop rows with no running time value and apply lambda fuction to convert any lists to strings
    box_office = wiki_movies_df['Box office'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # Defining regular expressions to identify desired values
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    # Remove any values between a dollar sign and a hyphen
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    # Funtion for converting string values into floats

    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a million
            value = float(s) * 10**6
            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a billion
            value = float(s) * 10**9
            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            # remove dollar sign and commas
            s = re.sub('\$|,','', s)
            # convert to float
            value = float(s)
            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan

    # Adding a column with the clean box office values by:
    # Extracting desired data and applying parse_dollars fuction
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # Removing old box office column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    print("Successfully cleaned Wikipedia box office data")
except:
    print("Failed to clean Wikipedia box office data")

# Cleaning budget data

try:
    # Drop rows with no running time value and apply lambda fuction to convert any lists to strings
    budget = wiki_movies_df['Budget'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # Remove any values between a dollar sign and a hyphen
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    # Relpacing numbers in brackets with a space
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    # Adding a column with the clean budget values by:
    # Extracting desired data and applying parse_dollars fuction
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # Removing old budget column
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    print("Successfully cleaned Wikipedia budget data")
except:
    print("Failed to clean Wikipedia budget data")

# Parsing release date and run time

try:
    # Drop rows with no release date value and apply lambda fuction to convert any lists to strings
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # Define desired formats
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    # Add a column with the clean release date values by: 
    # Using panda's built in datetime method to parse through the desired values
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    # Removing old release date column
    wiki_movies_df.drop('Release date', axis=1, inplace=True)
    # Drop rows with no running time value and apply lambda fuction to convert any lists to strings
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # Parsing Values that have h or hr in them
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    # Apply a function that will: 
    # convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero
    # Save it to new running_time column
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    # Drop old Running time column
    wiki_movies_df.drop('Running time', axis=1, inplace=True)

    print("Successfully parsed Wikipedia release date and run time")
except:
    print("Failed to parse Wikipedia release date and run time")

# Cleaning Kaggle Data

try:
    # Removing adult movie rows and corrupted data and dropping adult column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    # Coverting necessary data to correct data type
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

    print("Successfully cleaned Kaggle data")
except:
    print("Failed to clean Kaggle data")

# Merging Wikipedia and Kaggle Metadata

try:
    # Merging Wikipedia and Kaggle data and assigning suffixes
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    # Dropping incorrectly merged row
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    # Drop Wikipedia data columns that are insufficient compared to Kaggle data
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    # Function that fills missing Kaggle data with Wikipedia data and drops wiki column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
    # Call fuction on desired columns
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    # Drop video column because of insufficient data
    movies_df.drop('video', axis=1, inplace=True)

    # Reordering columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                        'genres','original_language','overview','spoken_languages','Country',
                        'production_companies','production_countries','Distributor',
                        'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                        ]]
    # Changing header name for consistency
    movies_df.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)

    print("Successfully merged Wikipidia and Kaggle data")
except:
    print("Failed to merge Wikipidia and Kaggle data")

# Loading Pandas DataFrame into SQL Database

try:
    # Create Database Engine and SQL movie data table
    connection_string = f"postgres://postgres:{sql_pw}@localhost:5432/movie_data"
    engine = create_engine(connection_string)
    movies_df.to_sql(name="movies", con=engine, if_exists="replace")
    
    print("Loaded Movies Data into SQL")
except:
    print("Failed to load Movies Data into SQL")

# ETL for Ratings Data

try:
    # Delete data from ratings table
    delete_statement = "DELETE FROM ratings"
    engine.connect().execute(delete_statement)
    # create a variable for the number of rows imported
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
        # Convert timestamp to seconds
        pd.to_datetime(data['timestamp'], unit='s')
        # print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        # increment the number of rows imported by the size of 'data'
        rows_imported += len(data)
        # print that the rows have finished importing
        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')
except:
    print("Failed to load Ratings Data into SQL")