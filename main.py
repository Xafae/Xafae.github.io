from fastapi import FastAPI
import pandas as pd

app = FastAPI()

#Loading csv files already cleaned and modified
df_streaming_services = pd.read_csv(r"MLOpsReviews\streaming_services_and_ratings.csv")

#Function to find the longest movie
@app.get("/max_duration")
async def get_max_duration(year: int = None, platform: str = None, duration_type: str = None):

    filtered_movies = df_streaming_services["type"=="movie"].copy()

    #Filtering
    if year:
        filtered_movies = filtered_movies[filtered_movies["release_year"] == year]

    if platform:
        filtered_movies = filtered_movies[filtered_movies["show_id"].str.startswith(platform[0].lower())]

    if duration_type:
        filtered_movies = filtered_movies[filtered_movies["duration_type"] == duration_type.lower()]

    #Looking for the one with highest duration
    max_duration_movie = filtered_movies.loc[filtered_movies["duration_int"].idxmax()]
    return {"movie_title": max_duration_movie["title"],
            "duration" : str(max_duration_movie["duration_int"]) + max_duration_movie["duration_type"]}


#Function to find the movie with filters of score and year
@app.get("/score_count")
async def get_score_count(platform: str = None, scored: float = None, year: int = None):
    #A movie/serie could have more than one rating, therefore I'm calculating a mean before moving forward.

    #Filtering
    filtered_df = df_streaming_services[(df_streaming_services["show_id"].str.startswith(platform[0].lower())) &
                            (df_streaming_services["score"] > scored) &
                            (df_streaming_services["release_year"] == year)&
                            (df_streaming_services["type"== "movie"])]
    
    return {"amount of movies with a higher score of "+scored: filtered_df["show_id"].nunique()}


#Function to count the amount of movies by platform
@app.get("/count_platform")
async def get_count_platform(platform: str = None):
    filtered_df = df_streaming_services[df_streaming_services["show_id"].str.startswith(platform[0].lower()) &
                                        df_streaming_services["type" == "movie"]]

    return {"amount of movies/series": filtered_df["show_id"].nunique()}


#Function to find the actor who appear most times
@app.get("/actor_repeated")
async def get_actor(platform: str = None, year: int = None):
    filtered_df = df_streaming_services.copy()
    
    if platform:
        filtered_df = filtered_df[filtered_df["show_id"].str.startswith(platform[0].lower())]

    if year:
        filtered_df = filtered_df[filtered_df["release_year"] == year]
    
    Top_actor = filtered_df["cast"].str.split(", ").explode().value_counts().index[0]

    return {"top actor" : Top_actor}