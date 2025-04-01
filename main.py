import streamlit as st
import pandas as pd
import query

st.set_page_config(page_title="NoSQL Project", layout="centered")

st.title("Projet réalisé par Brunet Pierre et Caton Quentin")
st.write("TD48")


if st.button("1)Afficher l'année où le plus grand nombre de films ont été sortis."):
    result=query.query1()
    if result:
        st.success(f"1) L'année avec le plus grand nombre de films est {result[0]['_id']} avec {result[0]['count']} films.")
    else:
        st.error("Aucun résultat trouvé.")


if st.button("2)Quel est le nombre de films sortis après l'année 1999."):
    count=query.query2()
    st.success(f"2) Le nombre de films sortis apres 1999 est : {count}")


if st.button("3)Quelle est la moyenne des votes des films sortis en 2007."):
    result=query.query3()
    if result and "average_votes" in result[0]:
        st.success(f"3) La moyenne des votes des films sortis en 2007 est {result[0]['average_votes']:.2f}.")
    else:
        st.error("Aucun film trouvé pour l'année 2007.")


if st.button("4)Affichez un histogramme qui permet de visualiser le nombres de films par année."):
    fig=query.query4()
    st.pyplot(fig)

if st.button("5)Quelles sont les genres de films disponibles dans la bases"):
    result=query.query5()
    if result and "genres" in result[0]:
        st.subheader("5) Genres disponibles dans la base de données")
        st.write(", ".join(sorted(result[0]["genres"])))  
    else:
        st.error("Aucun genre trouvé.")


if st.button("6)Quel est le film qui a généré le plus de revenu."):
    result=query.query6()
    for film in result:
        st.subheader(f"6) Le film ayant genere le plus de revenu est : {film['title']}")
        st.write(f"Revenu genere : {film['Revenue (Millions)']} millions")


if st.button("7)Quels sont les réalisateurs ayant réalisé plus de 5 films dans la base de données ?"):
    result=query.query7()
    if result:
        st.subheader("Réalisateurs avec plus de 5 films")
        df = pd.DataFrame(result)
        df.rename(columns={"_id": "Réalisateur", "nombre_films": "Nombre de films"}, inplace=True)
        st.table(df)
    else:
        st.error("Aucun réalisateur n'a réalisé plus de 5 films.")


if st.button("8)Quel est le genre de film qui rapporte en moyenne le plus de revenus ?"):
    result=query.query8()
    for genre in result:
        st.success(f"8) Le genre qui rapporte le plus en moyenne est {genre['_id']} avec {genre['average_revenue']:.2f} millions de dollars.")


if st.button("9)Quels sont les 3 films les mieux notés (rating) pour chaque décennie (1990-1999, 2000-2009,etc.) ?"):
    results = query.query9()
    if results:
        st.subheader("Top 3 films par décennie")
        st.table(results)
    else:
        st.error("Aucun film trouvé.")

if st.button("10)Quel est le film le plus long (Runtime) par genre ?"):
    result=query.query10()
    for film in result:
        st.success(f"10) Pour le genre {film['_id']}, le film le plus long est '{film['longest_movie']}' avec {film['runtime']} minutes.")


if st.button("11)Créer une vue MongoDB affichant uniquement les films ayant une note supérieure à 80(Metascore) et généré plus de 50 millions de dollars."):
    result=query.query11()
    if result:
        st.subheader("11) Films avec Metascore > 80 et Revenus > 50M :")
        for movie in result:
            st.write(f"- {movie['title']} (Metascore: {movie['Metascore']}, Revenue: {movie['Revenue (Millions)']}M)")
    else:
        st.error("Aucun film ne correspond aux critères.")


if st.button("12)Calculer la corrélation entre la durée des films (Runtime) et leur revenu (Revenue). (réaliser une analyse statistique.)"):
    df, pearson_corr, pearson_p, spearman_corr, spearman_p, fig = query.query12()

    # Affichage des résultats
    st.subheader("Résumé des données")
    st.dataframe(df.head(10))  # Affiche les 10 premières lignes

    st.subheader("Corrélation")
    st.write(f"**Pearson**: {pearson_corr:.4f} (p-value: {pearson_p:.4f})")
    st.write(f"**Spearman**: {spearman_corr:.4f} (p-value: {spearman_p:.4f})")

    # Affichage du graphique
    st.subheader("Graphique de la corrélation")
    st.pyplot(fig)


if st.button("13)Y a-t-il une évolution de la durée moyenne des films par décennie ?"):
    result = query.query13()
    if result:
        st.subheader("13) Évolution de la durée moyenne des films par décennie :")
        for entry in result:
            # Vérification si 'average_runtime' est None
            average_runtime = entry.get('average_runtime')
            if average_runtime is None:
                st.write(f"Décennie {entry['_id']} : Non disponible")
            else:
                st.write(f"Décennie {entry['_id']} : {average_runtime:.2f} minutes")
    else:
        st.error("Aucun film trouvé.")

st.title("Partie Neo4j")

if st.button("14)Quel est l'acteur ayant joue dans le plus grand nombre de films ?"):
    acteur, nb_films = query.query14()
    if acteur:
        st.success(f"L'acteur ayant joué dans le plus de films est **{acteur}** avec **{nb_films}** films !")
    else:
        st.warning("Aucun acteur trouvé.")

if st.button("15)Quels sont les acteurs ayant joué dans des films où l'actrice Anne Hathaway a également joué ?"):
    actors = query.query15()
    if actors:
        st.success(f"Acteurs ayant joué avec Anne Hathaway :")
        for actor in actors:
            st.write(f"- {actor}")
    else:
        st.warning("Aucun acteur trouvé.")

if st.button("16)Quel est l'acteur ayant joué dans des films totalisant le plus de revenus ?"):
    actor, revenue = query.query16()
    if actor:
        st.success(f"L'acteur ayant joué dans les films générant le plus de revenus est **{actor}** avec **{revenue:.2f} millions** de dollars.")
    else:
        st.warning("Aucun acteur trouvé.")

if st.button("17)Quelle est la moyenne des votes ?"):
    moyenne = query.query17()
    if moyenne is not None:
        st.success(f"La moyenne des votes des films est de : {moyenne:.2f}")
    else:
        st.error("Impossible de calculer la moyenne des votes.")

if st.button("18)Quel est le genre le plus représenté dans la base de données ?"):
    genre, count = query.query18()
    if genre:
        st.success(f"Le genre le plus représenté est : {genre} ({count} films)")
    else:
        st.error("Impossible de déterminer le genre le plus représenté.")
    
if st.button("19)Quels sont les films dans lesquels les acteurs ayant joué avec vous ont également joué ?"):
    results = query.query19()
    if results:
        for record in results:
            film_title, co_actors = record["film_title"], record["co_actors"]
            st.write(f"Film: {film_title}, Acteurs: {', '.join(co_actors)}")
    else:
        st.error("Aucun film trouvé pour les acteurs spécifiés.")

if st.button("20)Quel réalisateur Director a travaillé avec le plus grand nombre d'acteurs distincts ?"):
    realisateur_top = query.query20()
    if realisateur_top:
        st.success(f"**{realisateur_top['realisateur']}** est le réalisateur ayant collaboré avec le plus d'acteurs : **{realisateur_top['nombre_acteurs']}** acteurs !")
    else:
        st.warning("Aucun réalisateur trouvé.")

if st.button("21)Quels sont les films les plus ”connectés”, c'est-à-dire ceux qui ont le plus d'acteurs en commun avec d'autres films ?"):
    films = query.query21()
    if films:
        for film in films:
            st.write(f"**{film['film']}** - {film['common_actors']} acteurs en commun")
    else:
        st.warning("Aucun film trouvé.")

if st.button("22)Trouver les 5 acteurs ayant joué avec le plus de réalisateurs différents"):
    actors = query.query22()
    if actors:
        for actor in actors:
            st.write(f"**{actor['acteur']}** - {actor['nb_realisateurs']} réalisateurs")
    else:
        st.warning("Aucun acteur trouvé.")

if "show_input" not in st.session_state:
    st.session_state.show_input = False
# Bouton pour afficher le champ de texte
if st.button("23)Recommander un film à un acteur en fonction des genres des films où il a déjà joué."):
    st.session_state.show_input = True  # Activer l'affichage du champ de texte
# Affichage conditionnel du champ de texte
if st.session_state.show_input:
    actor_name = st.text_input("Nom de l'acteur :", "")
    if actor_name:  # Vérifier si un nom a été entré
        recommendations = query.query23(actor_name)
        if recommendations:
            st.subheader(f"Films recommandés pour {actor_name}")
            for rec in recommendations:
                st.write(f"**{rec['film']}** - Genres : {', '.join(rec['genres'])}")
        else:
            st.warning("Aucune recommandation trouvée.")