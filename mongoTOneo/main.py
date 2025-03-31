from extract import extract_films, extract_directors, extract_actors
from transform import clean_films_data
from load import load_films, load_directors, load_actors, create_relationships

def main():
    print("Extraction des donnes de MongoDB...")
    films = extract_films()
    directors = extract_directors()
    actors = extract_actors()

    print("Transformation des donnees...")
    cleaned_films = clean_films_data(films)

    print("Chargement des donnees dans Neo4j...")
    load_films(cleaned_films)
    load_directors(directors)
    load_actors(actors)

    print("Creation des relations...")
    create_relationships(cleaned_films)

    print("Migration terminee !")

if __name__ == "__main__":
    main()
