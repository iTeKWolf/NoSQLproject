def clean_films_data(films):
    cleaned_films = []
    for film in films:
        cleaned_film={
            "id": str(film["_id"]),
            "title": film.get("title","Inconnus"),
            "year": film.get("year", None),
            "votes": int(film.get("Votes", 0)),
            "revenue": film.get("Revenue (Millions)", 0),
            "rating": film.get("rating", "N/A"),
            "director": film.get("Director", None),
            "actors": [actor.strip() for actor in film.get("Actors", "").split(",")] if film.get("Actors") else []
        }
        #print(cleaned_film)#debug
        cleaned_films.append(cleaned_film)
    return cleaned_films
