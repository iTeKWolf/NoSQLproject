from pymongo import MongoClient
from pymongo.server_api import ServerApi
import streamlit as st
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import scipy.stats as stats

import matplotlib
matplotlib.use("Agg")

import database
db=database.get_db()
collection=database.get_collection()
session=database.get_session()


#Query 1
####################################################
def query1():
    result = list(collection.aggregate([
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, 
        {"$limit": 1}
    ]))

    return result

#Query 2
####################################################
def query2():
    count = collection.count_documents({"year":{"$gt":1999}})
    return count

#Query 3
####################################################
def query3():
    pipeline = [
        {"$match": {"year": 2007}}, 
        {"$group": {"_id": None, "average_votes": {"$avg": "$Votes"}}} 
    ]


    result = list(collection.aggregate(pipeline))
    return result


#Query 4
####################################################
def query4():
    years = [film.get('year',None) for film in collection.find() if 'year' in film]
    years_counts = Counter(years)
    years_sorted = sorted(years_counts.keys())
    counts_sorted = [years_counts[years] for years in years_sorted]

    df = pd.DataFrame({
        'Year': years_sorted,
        'Count': counts_sorted
    })
    # Création du graphique
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x='Year', y='Count', data=df, color='skyblue', ax=ax)
    ax.set_xlabel('Année')
    ax.set_ylabel('Nombre de films')
    ax.set_title('Nombre de films par année')
    plt.xticks(rotation=45)
    plt.tight_layout()
    return fig

#Query 5
####################################################
def query5():
    pipeline = [
        {"$project": {"genres": {"$split": ["$genre", ","]}}},
        {"$unwind": "$genres"}, 
        {"$group": {"_id": None, "genres": {"$addToSet": "$genres"}}} 
    ]

    result = list(collection.aggregate(pipeline))
    return result

#Query 6
####################################################
def query6():
    #result = collection.find().sort("Revenue (Millions)", -1).limit(1) plus simple mais moins perf que aggregate
    result = collection.aggregate([
        {"$match": {"Revenue (Millions)": {"$exists": True, "$ne": None, "$ne": ""}}},
        {"$sort": {"Revenue (Millions)": -1}},
        {"$limit": 1}
    ])
    return result

#Query 7
####################################################
def query7():
    pipeline = [
        {"$match": {"Director": {"$exists": True, "$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$Director", "nombre_films": {"$sum": 1}}},
        {"$match": {"nombre_films": {"$gt": 5}}}, 
        {"$sort": {"nombre_films": -1}}  
    ]


    result = list(collection.aggregate(pipeline))
    return result

#Query 8
####################################################
def query8():
    result = collection.aggregate([
        {"$match": {"Revenue (Millions)": {"$exists": True, "$ne": None, "$ne": ""}}},
        {"$unwind": "$genre"},
        {"$group": { "_id": "$genre", "average_revenue": {"$avg": "$Revenue (Millions)"}}},
        {"$sort": {"average_revenue": -1}},
        {"$limit": 1}
    ])
    return result
    

#Query 9
####################################################
def query9():
    pipeline = [
        {"$addFields": {"decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}}}, 
        {"$sort": {"decade": 1, "rating": -1}},
        {"$group": {
            "_id": "$decade",
            "top_movies": {"$push": {"title": "$title", "rating": "$rating"}}
        }},
        {"$project": {
            "_id": 1,
            "top_movies": {"$slice": ["$top_movies", 3]}
        }},
        {"$sort": {"_id": 1}} 
    ]

    result = list(collection.aggregate(pipeline))
    # Fonction pour essayer de convertir le rating en nombre
    # Convertir les résultats pour Streamlit
    formatted_result = []
    for entry in result:
        decade = entry["_id"]
        for movie in entry["top_movies"]:
            title = movie.get("title", "Inconnu")
            rating = movie.get("rating", "N/A")
            formatted_result.append({"Décennie": decade, "Titre": title, "Note": rating})

    return formatted_result  # Retourner une liste de dictionnaires

#Query 10
####################################################
def query10():
    result = collection.aggregate([
        {"$match": {"Runtime (Minutes)": {"$exists": True, "$ne": None, "$ne": ""}}},  # Filtrer les films avec durée valide
        {"$unwind": "$genre"},  # Séparer les genres (si c'est une liste)
        {"$sort": {"Runtime (Minutes)": -1}},  # Trier par durée décroissante
        {"$group": {
            "_id": "$genre",  
            "longest_movie": {"$first": "$title"},  # Prendre le film avec la plus grande durée
            "runtime": {"$first": "$Runtime (Minutes)"}  # Récupérer la durée correspondante
        }},
        {"$sort": {"runtime": -1}}  # Trier par durée décroissante (optionnel)
    ])
    return result

#Query 11
####################################################
def query11():
    if "top_movies_view" in db.list_collection_names():
        db.drop_collection("top_movies_view")


    db.create_collection(   
        "top_movies_view",
        viewOn="films",
        pipeline=[
            { "$match": { "Metascore": { "$gt": 80 }, "Revenue (Millions)": { "$gt": 50 } } }
        ]
    )


    top_movies_view = db["top_movies_view"]

    result = list(top_movies_view.find())
    return result

#Query 12
####################################################
def query12():
    result = collection.find(
        {"Runtime (Minutes)": {"$exists": True, "$ne": None, "$ne": ""},"Revenue (Millions)": {"$exists": True, "$ne": None, "$ne": ""}},
        {"Runtime (Minutes)": 1, "Revenue (Millions)": 1, "_id": 0}
    )
    films = list(result)

    df = pd.DataFrame(films)
    # Convertir les valeurs en numériques
    df["Runtime (Minutes)"] = pd.to_numeric(df["Runtime (Minutes)"], errors="coerce")
    df["Revenue (Millions)"] = pd.to_numeric(df["Revenue (Millions)"], errors="coerce")
    # Supprimer les valeurs NaN après conversion
    df.dropna(inplace=True)
    # Calcul de la corrélation de Pearson
    pearson_corr, pearson_p = stats.pearsonr(df["Runtime (Minutes)"], df["Revenue (Millions)"])
    # Calcul de la corrélation de Spearman
    spearman_corr, spearman_p = stats.spearmanr(df["Runtime (Minutes)"], df["Revenue (Millions)"])
    
    # Création du graphique
    fig, ax = plt.subplots(figsize=(10,6))
    sns.regplot(x=df["Runtime (Minutes)"], y=df["Revenue (Millions)"], 
                scatter_kws={"alpha":0.5}, line_kws={"color":"red"}, ax=ax)
    ax.set_title("Correlation entre la durée des films et leur revenu")
    ax.set_xlabel("Durée du film (minutes)")
    ax.set_ylabel("Revenu (Millions)")

    return df, pearson_corr, pearson_p, spearman_corr, spearman_p, fig

#Query 13
####################################################
def query13():
    pipeline = [
        {"$addFields": {"decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}}}, 
        {"$group": {
            "_id": "$decade",
            "average_runtime": {"$avg": "$Runtime (Minutes)"}
        }},
        {"$sort": {"_id": 1}}  
    ]

    result = list(collection.aggregate(pipeline))
    return result

#Query 14
####################################################
def query14():
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    RETURN a.name AS acteur, COUNT(f) AS nb_films
    ORDER BY nb_films DESC
    LIMIT 1
    """
    result = session.run(query)
    record = result.single()
    return record["acteur"], record["nb_films"] if record else (None, 0)

#Query 15
####################################################
def query15():
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(anne:Actor {name: "Anne Hathaway"})
    WHERE a.name <> "Anne Hathaway"
    RETURN DISTINCT a.name AS acteur
    ORDER BY acteur;
    """
    result = session.run(query)
    return [record["acteur"] for record in result]

#Query 16
####################################################
def query16():
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    WHERE f.revenue IS NOT NULL AND f.revenue <> "N/A"
    RETURN a.name AS acteur, SUM(coalesce(toFloat(f.revenue), 0)) AS total_revenu
    ORDER BY total_revenu DESC
    LIMIT 1;
    """
    result = session.run(query)
    record = result.single()
    return record["acteur"], record["total_revenu"] if record else (None, 0)

#Query 17
####################################################
def query17():
    query = """
    MATCH (f:Film)
    WHERE f.votes IS NOT NULL AND f.votes <> "N/A"
    RETURN avg(toFloat(f.votes)) AS moyenne_votes;
    """
    result = session.run(query)
    return result.single()["moyenne_votes"]
    
#Query 18
####################################################
def query18():
    query = """
    MATCH (f:Film)
    UNWIND f.genre AS genre
    RETURN genre, COUNT(*) AS nombre_films
    ORDER BY nombre_films DESC
    LIMIT 1;
    """
    result = session.run(query)
    return result.single()
    
#Query 19
####################################################
#Query 20
####################################################
#Query 21
####################################################
#Query 22
####################################################
#Query 23
####################################################
#Query 24
#####################################################
#Query 25
#####################################################
#Query 26
####################################################
