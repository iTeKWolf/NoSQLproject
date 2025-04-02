from database import get_collection
import re


collection = get_collection()

def extract_films():
    films = collection.find({}, {"_id": 1, "title": 1, "year": 1, "Votes": 1, "Revenue (Millions)": 1, "rating": 1, "Director": 1, "Actors": 1, "genre": 1})
    return list(films)

def extract_directors():
    directors = collection.distinct("Director")
    return [{"name": director} for director in directors if director]

def extract_actors():
    actors = collection.distinct("Actors")
    #flat_list = {actor for sublist in actors if sublist for actor in sublist.split(", ")}
    flat_list = {actor.strip() for sublist in actors if sublist for actor in re.split(r',\s*', sublist)}
    return [{"name": actor} for actor in flat_list]
