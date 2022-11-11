import requests
import json
from pymongo import MongoClient

URI_MOVIES = 'https://api.themoviedb.org/3/movie/popular?api_key=<apikey>&language=en-US&page='
URI_SERIES = 'https://api.themoviedb.org/3/tv/popular?api_key=<apikey>&language=en-US&page='
URI_GAMES = "https://api.rawg.io/api/games?key=<apikey>c&page_size=40&page="

mongo_url = "url to db"

client = MongoClient(mongo_url)

db = client['db name']


def parse_films(database, kind):
    films = database[kind]

    for i in range(500):
        page = i + 1

        res = requests.get(URI_MOVIES + str(page))
        results = json.loads(res.text)['results']

        for movie in results:
            movie.update({
                "page": page
            })

        films.insert_many(results)
        print("Page " + str(page) + " loaded" + "                 ( " + str((page/500)*100) + "% )")

    print("done")


def parse_games(database):
    total_pages = 10000

    games_db = database["Games-API"]

    games_to_parse = []

    for page in range(total_pages):
        games = json.loads(requests.get(URI_GAMES + str(page + 1)).text)['results']

        for game in games:
            genres = []
            platforms = []

            for genre in game['genres']:
                genres.append(genre['name'])

            for platform in game['parent_platforms']:
                platforms.append(platform['platform']['name'])

            game_model = {
                "page": str(page + 1),
                "id": game['id'],
                "background_image": game['background_image'],
                "genres": genres,
                "platforms": platforms,
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
        parse_films(db, "Films collection")

    elif operation == 2:
        parse_films(db, "Series collection")

    elif operation == 3:
        parse_games(db)

    else:
        print("No kind operation exists")
