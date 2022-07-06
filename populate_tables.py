import sqlite3
import random as rd
import datetime
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_client(conn, client):
    """
    Create a new client
    :param conn:
    :param client:
    :return: client id
    """
    sql = """ INSERT INTO clients(name)
              VALUES(?) """
    cur = conn.cursor()
    cur.execute(sql, (client,))
    conn.commit()
    return cur.lastrowid


def create_movie(conn, movie):
    """
    Create a new movie
    :param conn:
    :param movie:
    :return:
    """

    sql = """ INSERT INTO movies(name)
              VALUES(?) """
    cur = conn.cursor()
    cur.execute(sql, (movie,))
    conn.commit()
    return cur.lastrowid


def create_rent(conn, rent):
    """
    Create a new rent
    :param conn:
    :param rent:
    :return:
    """

    sql = """ INSERT INTO rents(client_id,movie_id,begin_date,end_date)
              VALUES(?,?,?,?) """
    cur = conn.cursor()
    cur.execute(sql, rent)
    conn.commit()
    return cur.lastrowid


def main():
    database = r"db\database.db"

    # create a database connection
    conn = create_connection(database)
    with conn:
        # create a new set of clients
        clients = [
            "Beyond See",
            "Bradley Pits",
            "John Smith",
            "Maicol Scot",
            "Bartolomew Obame",
        ]
        clients_id = []
        for client in clients:
            id = create_client(conn, client)
            clients_id.append(id)
            print(f"{client} created with id:{id}")

        # create a new set of movies
        movies = [
            "Titan INC",
            "Men in Grey",
            "Wasp Movie",
            "The Green Ogre and the Talking Donkey",
            "Revengers",
        ]
        movies_id = []
        for movie in movies:
            id = create_movie(conn, movie)
            movies_id.append(id)
            print(f"{movie} created with id:{id}")

        # rent
        start_date = datetime.date(
            year=rd.randint(2000, 2022), month=rd.randint(1, 12), day=rd.randint(1, 28)
        )
        end_date = start_date + datetime.timedelta(days=30)
        rent = (
            clients_id[rd.randrange(0, len(clients_id), 1)],
            movies_id[rd.randrange(0, len(movies_id), 1)],
            str(start_date),
            str(end_date),
        )

        # create rent
        create_rent(conn, rent)


if __name__ == "__main__":
    main()
