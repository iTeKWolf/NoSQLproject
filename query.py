from pymongo import MongoClient
from pymongo.server_api import ServerApi
import streamlit as st
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import scipy.stats as stats

import matplotlib
matplotlib.use("TkAgg")

import database
db=database.get_db()
collection=database.get_collection()


#Query 1
####################################################
def query1():
    result = list(collection.aggregate([
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, 
        {"$limit": 1}
    ]))

    if result:
        print(f"1)L'année avec le plus grand nombre de films est {result[0]['_id']} avec {result[0]['count']} films.")
    else:
        print("Aucun résultat trouvé.")

#Query 2
####################################################
def query2():
    count = collection.count_documents({"year":{"$gt":1999}})
    print(f"2)Le nombre de films sortis apres 1999 est : {count}")

#Query 3
####################################################
def query3():
    pipeline = [
        {"$match": {"year": 2007}}, 
        {"$group": {"_id": None, "average_votes": {"$avg": "$Votes"}}} 
    ]


    result = list(collection.aggregate(pipeline))

    if result and "average_votes" in result[0]:
        print(f"3)La moyenne des votes des films sortis en 2007 est {result[0]['average_votes']:.2f}.")
    else:
        print("Aucun film trouvé pour l'année 2007.")


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
    plt.figure(figsize=(10, 6))
    sns.barplot(x='Year', y='Count', data=df, color='skyblue')
    plt.xlabel('Année')
    plt.ylabel('Nombre de films')
    plt.title('4)Nombre de films par année')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

#Query 5
####################################################
def query5():
    pipeline = [
        {"$project": {"genres": {"$split": ["$genre", ","]}}},
        {"$unwind": "$genres"}, 
        {"$group": {"_id": None, "genres": {"$addToSet": "$genres"}}} 
    ]


    result = list(collection.aggregate(pipeline))


    if result and "genres" in result[0]:
        print("5)Genres disponibles dans la base de données :")
        print(", ".join(sorted(result[0]["genres"]))) 
    else:
        print("Aucun genre trouvé.")

#Query 6
####################################################
def query6():
    #result = collection.find().sort("Revenue (Millions)", -1).limit(1) plus simple mais moins perf que aggregate
    result = collection.aggregate([
        {"$match": {"Revenue (Millions)": {"$exists": True, "$ne": None, "$ne": ""}}},
        {"$sort": {"Revenue (Millions)": -1}},
        {"$limit": 1}
    ])
    for film in result:
        print(f"6)Le film ayant genere le plus de revenu est : {film['title']}")
        print(f"Revenu genere : {film['Revenue (Millions)']} millions")

#Query 7
####################################################
def query7():
    pipeline = [
        {"$group": {"_id": "$Director", "nombre_films": {"$sum": 1}}},
        {"$match": {"nombre_films": {"$gt": 5}}}, 
        {"$sort": {"nombre_films": -1}}  
    ]


    result = list(collection.aggregate(pipeline))

    if result:
        print("7)Réalisateurs ayant réalisé plus de 5 films :")
        for entry in result:
            print(f"- {entry['_id']} : {entry['nombre_films']} films")
    else:
        print("Aucun réalisateur n'a réalisé plus de 5 films.")

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
    for genre in result:
        print(f"8)Le genre qui rapporte le plus en moyenne est {genre['_id']} avec {genre['average_revenue']:.2f} millions de dollars.")

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
    def get_numeric_rating(rating):
        try:
            # Si le rating est numérique, on le convertit en float
            if rating.lower() in ["unrated", "na", "not rated"]:
                return "N/A"
            return float(rating)  # Convertir en float
        except ValueError:
            return rating  # Si c'est une chaîne qui ne peut pas être convertie, on garde la chaîne

    if result:
        print("9)Top 3 films par décennie :")
        for entry in result:
            print(f"\nDécennie {entry['_id']} :")
            for movie in entry["top_movies"]:
                title = movie.get("title", "Inconnu")  # Valeur par défaut si 'title' est manquant
                rating = get_numeric_rating(movie.get("rating", "N/A"))  # Traiter la note correctement
                print(f"- {title} (Note: {rating})")
    else:
        print("Aucun film trouvé.")

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

    for film in result:
        print(f"10)Pour le genre {film['_id']}, le film le plus long est '{film['longest_movie']}' avec {film['runtime']} minutes.")

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

    if result:
        print("11)Films avec Metascore > 80 et Revenus > 50M :")
        for movie in result:
            print(f"- {movie['title']} (Metascore: {movie['Metascore']}, Revenue: {movie['Revenue (Millions)']}M)")
    else:
        print("Aucun film ne correspond aux critères.")

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
    # Afficher les premières lignes pour vérifier
    print(df.head())
    # Calcul de la corrélation de Pearson
    pearson_corr, pearson_p = stats.pearsonr(df["Runtime (Minutes)"], df["Revenue (Millions)"])
    print(f"12)Correlation de Pearson: {pearson_corr:.4f} (p-value: {pearson_p:.4f})")
    # Calcul de la corrélation de Spearman
    spearman_corr, spearman_p = stats.spearmanr(df["Runtime (Minutes)"], df["Revenue (Millions)"])
    print(f"12)Correlation de Spearman: {spearman_corr:.4f} (p-value: {spearman_p:.4f})")

    # Création d'un scatter plot avec régression
    plt.figure(figsize=(10,6))
    sns.regplot(x=df["Runtime (Minutes)"], y=df["Revenue (Millions)"], scatter_kws={"alpha":0.5}, line_kws={"color":"red"})
    plt.title("Correlation entre la durée des films et leur revenu")
    plt.xlabel("Durée du film (minutes)")
    plt.ylabel("Revenu (Millions)")
    plt.show()

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

    if result:
        print("13)Évolution de la durée moyenne des films par décennie :")
        for entry in result:
            # Vérification si 'average_runtime' est None
            average_runtime = entry.get('average_runtime')
            if average_runtime is None:
                print(f"Décennie {entry['_id']} : Non disponible")
            else:
                print(f"Décennie {entry['_id']} : {average_runtime:.2f} minutes")
    else:
        print("Aucun film trouvé.")