import sqlite3
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


def update_rent(conn, rent):
    """
    update begin_date, and end date of a rent
    :param conn:
    :param rent:
    :return:
    """
    sql = """ UPDATE rents
              SET begin_date = ? ,
                  end_date = ?
              WHERE id = ?"""
    cur = conn.cursor()
    cur.execute(sql, rent)
    conn.commit()


def delete_rent(conn, id):
    """
    Delete a rent by rent id
    :param conn:  Connection to the SQLite database
    :param id: id of the rent
    :return:
    """
    sql = "DELETE FROM rents WHERE id=?"
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()


def delete_all_rents(conn):
    """
    Delete all rows in the rents table
    :param conn: Connection to the SQLite database
    :return:
    """
    sql = "DELETE FROM rents"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def select_all_rents(conn):
    """
    Query all rows in the rents table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM rents")

    rows = cur.fetchall()

    for row in rows:
        print(row)


def select_rent_by_end_date(conn, end_date):
    """
    Query rents by end_date
    :param conn: the Connection object
    :param end_date:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM rents WHERE end_date=?", (end_date,))

    rows = cur.fetchall()

    for row in rows:
        print(row)


def main():
    database = r"db\database.db"

    # create a database connection
    conn = create_connection(database)
    start_date = "2022-05-27"
    end_date = "2022-07-06"
    rent = (
        1,
        3,
        start_date,
        end_date,
    )
    start_date = "2022-05-27"
    end_date = "2022-07-07"
    updated_rent = (
        start_date,
        end_date,
        1,
    )
    with conn:
        delete_all_rents(conn)
        create_rent(conn, rent)
        update_rent(conn, updated_rent)
        select_all_rents(conn)
        select_rent_by_end_date(conn, end_date)
        delete_rent(conn, 1)


if __name__ == "__main__":
    main()
