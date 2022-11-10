import requests
import json
from pymongo import MongoClient

URI_MOVIES = 'https://api.themoviedb.org/3/tv/popular?api_key=5c72a972a13c78f3b464483f0fe30a39&language=en-US&page='
URI_GAMES = "https://api.rawg.io/api/games?key=bee48918ad63447fb75d085b1f624f6c&page_size=40&page="

mongo_url = "mongodb+srv://Axenos:G3E9yqondrevFHeJ@mazanoapi.tfz45ds.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(mongo_url)

db = client['Mazano-API']


def parse_films(database, kind):
    films = db[kind]

    for i in range(500):
        page = i + 1

        res = requests.get(URI_MOVIES + str(page))
        results = json.loads(res.text)['results']

        films.insert_many(results)
        print("Page " + str(page) + " loaded" + "                 ( " + str((page/500)*100) + "% )")

    print("done")


def parse_games(database):
    total_games = json.loads(requests.get(URI_GAMES + "1").text)['count']
    total_pages = int(total_games / 40) + 1

    games_db = database["Games-API"]

    games_to_parse = []

    for page in range(total_pages):
        games = json.loads(requests.get(URI_GAMES + str(page + 1)).text)['results']

        for game in games:
            genres = []

            for genre in game['genres']:
                genres.append(genre['name'])

            game_model = {
                "game_id": game['id'],
                "background_image": game['background_image'],
                "genres": genres,
                "language": "eng",
                "title": game['name'],
                "rating": game['rating']
            }

            if len(games_to_parse) == 1600:
                games_db.insert_many(games_to_parse)
                print("parsing 1600 games")
                games_to_parse = []
                print("cleaning cache")

            games_to_parse.append(game_model)

        print("page " + str(page + 1) + " loaded            " + str(page / total_pages * 100) + "%")


if __name__ == "__main__":
    operation = int(input("Choose operation: "
                          "\n     - 1 to load films "
                          "\n     - 2 to load series "
                          "\n     - 3 to load games \n1"))

    if operation == 1:
        parse_films(db, "Films-API")

    if operation == 2:
        parse_films(db, "Series-API")

    if operation == 3:
        parse_games(db)
