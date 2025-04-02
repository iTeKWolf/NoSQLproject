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
        m.director = film.director,
         m.genre = COALESCE(film.genre, [])
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
    queries = ["""
        MATCH (d:Director), (f:Film)
        WHERE d.name = f.director
        MERGE (d)-[:A_REALISE]->(f);
        """,
        """
        UNWIND $films AS film
        MATCH (m:Film {id: film.id})
        UNWIND film.actors AS actor_name
        MATCH (a:Actor {name: actor_name})
        MERGE (a)-[:A_JOUE]->(m)
        """,
        """
        MATCH (d1:Director)-[:A_REALISE]->(f1:Film),
            (d2:Director)-[:A_REALISE]->(f2:Film)
        WHERE d1 <> d2
        WITH d1, d2, COLLECT(DISTINCT f1.genre) AS genres1, COLLECT(DISTINCT f2.genre) AS genres2
        WITH d1, d2, apoc.coll.flatten(apoc.coll.intersection(genres1, genres2)) AS common_genres
        WHERE SIZE(common_genres) > 1
        MERGE (d1)-[r:INFLUENCE_PAR]->(d2)
        ON CREATE SET r.common_genres = common_genres;
        """,
        """
        MATCH (d1:Director)-[:A_REALISE]->(f1:Film)
        MATCH (d2:Director)-[:A_REALISE]->(f2:Film)
        WHERE d1 <> d2
            AND f1.year = f2.year
            AND ANY(genre IN f1.genre WHERE genre IN f2.genre)
            AND d1.name < d2.name  // Ensures a single relation between d1 and d2, no duplicates
        CREATE (d1)-[:CONCURRENCE]->(d2)
        RETURN d1.name AS Director1, d2.name AS Director2, f1.year AS Year, f1.genre AS Common_Genre
        """
    ]
    with get_session() as session:
        for query in queries:
            session.run(query, films=films)

def add_Nous():
    query = """
    MERGE (q:Actor {name: "Quentin Caton"})
    MERGE (p:Actor {name: "Pierre Brunet"})
    MERGE (m:Film {title: "Deadpool"})
    MERGE (q)-[:A_JOUE]->(m)
    MERGE (p)-[:A_JOUE]->(m)
    """
    
    with get_session() as session:
        session.run(query)

