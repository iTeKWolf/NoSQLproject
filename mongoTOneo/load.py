from database import get_session

def load_films(films):
    query = """
    UNWIND $films AS film
    MERGE (m:Film {id: film.id})
    ON CREATE SET 
        m.title = film.title, 
        m.year = film.year, 
        m.votes = film.votes,
        m.revenue = film.revenue, 
        m.rating = film.rating,
        m.director = film.director
    ON MATCH SET 
        m.year = COALESCE(film.year, m.year),
        m.votes = COALESCE(film.votes, m.votes),
        m.revenue = COALESCE(film.revenue, m.revenue),
        m.rating = COALESCE(film.rating, m.rating)
    """
    with get_session() as session:
        session.run(query, films=films)

def load_directors(directors):
    query = """
    UNWIND $directors AS director
    MERGE (d:Director {name: director.name})
    """
    with get_session() as session:
        session.run(query, directors=directors)

def load_actors(actors):
    query = """
    UNWIND $actors AS actor
    MERGE (a:Actor {name: actor.name})
    """
    with get_session() as session:
        session.run(query, actors=actors)

def create_relationships(films):
    queries = [
        """
        MATCH (m:Film), (d:Director {name: m.director})
        MERGE (d)-[:A_REALISE]->(m)
        """,
        """
        UNWIND $films AS film
        MATCH (m:Film {id: film.id})
        UNWIND film.actors AS actor_name
        MATCH (a:Actor {name: actor_name})
        MERGE (a)-[:A_JOUE]->(m)
        """
    ]
    with get_session() as session:
        for query in queries:
            session.run(query, films=films)

