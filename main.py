import psycopg2

if __name__ == '__main__':
    print("esempi sql")

    try:
        connection = psycopg2.connect(user="postgres",
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="sample")

        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE students
              (ID SERIAL   ,
              NAME           TEXT    NOT NULL,
              SURNAME         TEXT NOT NULL); '''

        cursor.execute(create_table_query)

        nomi = [("Marco", "Torlaschi"), ("Roberta", "Latini"), ("Giorgo", "Franzini")]

        insert = "INSERT INTO students (name,surname) VALUES (%s,%s)"

        connection.commit()

        for n in nomi:
            cursor.execute(insert, n)

        cursor.execute("SELECT * FROM  students  ")
        for r in cursor.fetchall():
            print(r, "\n")

        print("adsd")

        cursor.execute("DELETE FROM  students where name='Roberta' ")

        cursor.execute("SELECT * FROM  students  ")
        for r in cursor.fetchall():
            print(r, "\n")


        cursor.execute("DELETE FROM  students  ")

        cursor.execute("DROP TABLE  students  ")

        connection.commit()



    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
