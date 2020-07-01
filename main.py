import psycopg2


def create_table(connection):
    # il cursore è utillizato esequire comandi e accedere alla risposta
    cursor = connection.cursor()

    try:

        # prepara il comando
        create_table_query = '''CREATE TABLE students
              (ID SERIAL   PRIMARY KEY ,
              NAME           TEXT    NOT NULL,
              SURNAME         TEXT NOT NULL,
              unique(NAME,SURNAME)); '''

        # eseguo il comando
        cursor.execute(create_table_query)

        create_table_query = '''CREATE TABLE courses
              (ID SERIAL  PRIMARY KEY ,
              NAME           TEXT    NOT NULL,
              DESCR        TEXT NOT NULL); '''

        cursor.execute(create_table_query)

        create_table_query = '''CREATE TABLE booking
              (STUDENT_ID BIGINT  ,
              COURSE_ID           BIGINT ,
              PRIMARY KEY (STUDENT_ID,COURSE_ID),
              FOREIGN KEY (STUDENT_ID) REFERENCES students(ID) MATCH FULL ON DELETE CASCADE,
              FOREIGN KEY (COURSE_ID) REFERENCES courses(ID) MATCH FULL ON DELETE CASCADE
              ); '''

        cursor.execute(create_table_query)

        # le modifiche diventano definitive
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error creating tables", error)
    finally:
        # chiudo il cursore in finally
        cursor.close()


def insert_names(connection):
    cursor = connection.cursor()

    try:

        nomi = [("Marco", "Torlaschi"), ("Roberta", "Latini"), ("Giorgo", "Franzini")]

        # preparo il comando generico con %s e poi inserisco  valori in all'esecuzione
        insert = "INSERT INTO students (NAME,SURNAME) VALUES (%s,%s)"

        for n in nomi:
            cursor.execute(insert, n)

        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error inserting names", error)
    finally:
        cursor.close()


def insert_courses(connection):
    cursor = connection.cursor()

    try:

        corsi = [("SQL", "corso sql"), ("SPRING", "corso spring")]

        insert = "INSERT INTO courses (NAME,DESCR) VALUES (%s,%s)"

        for n in corsi:
            cursor.execute(insert, n)

        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error inserting names", error)
    finally:
        cursor.close()


def insert_booking(connection, name, surname, course):
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT ID FROM  students  WHERE NAME=%s AND SURNAME=%s", (name, surname))
        student_id = cursor.fetchone()[0]
        cursor.execute("SELECT ID FROM  courses  WHERE NAME = %s ", (course,))
        course_id = cursor.fetchone()[0]

        cursor.execute("INSERT INTO booking (STUDENT_ID,COURSE_ID) VALUES (%s,%s)", (str(student_id), str(course_id)))

        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error inserting bookings", error)
    finally:
        cursor.close()


def delate_names(connetion):
    cursor = connection.cursor()

    try:
        cursor.execute("DELETE FROM  students where name='Roberta' ")

        # connection.rollback()
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error deleting name", error)
    finally:
        cursor.close()


def drop_tables(connetion):
    cursor = connection.cursor()

    try:
        cursor.execute("DROP TABLE  booking  ")
        cursor.execute("DROP TABLE  students  ")
        cursor.execute("DROP TABLE  courses  ")
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error deleting name", error)
    finally:
        cursor.close()


if __name__ == '__main__':
    print("esempi sql")

    try:

        # apro la connessione
        connection = psycopg2.connect(user="postgres",
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="sample")

        #drop_tables(connection)

        create_table(connection)

        insert_names(connection)

        insert_courses(connection)


        insert_booking(connection, "Marco", "Torlaschi", "SQL")
        insert_booking(connection, "Marco", "Torlaschi", "SPRING")
        insert_booking(connection, "Roberta", "Latini", "SPRING")

        delate_names(connection)

        cursor = connection.cursor()

        cursor.execute("SELECT * FROM  students  ")
        for r in cursor.fetchall():
            print(r, "\n")

        cursor.execute("SELECT * FROM  courses  ")
        for r in cursor.fetchall():
            print(r, "\n")

        cursor.execute("SELECT * FROM  booking  ")
        for r in cursor.fetchall():
            print(r, "\n")

        drop_tables(connection)


    except (Exception, psycopg2.Error) as error:
        # catcho l'errore in connessione
        print("Error while connecting to PostgreSQL", error)
    finally:
        # chiudo la connessione in finally per essere sicuro che avvenga sempre
        if (connection):
            connection.close()
            print("PostgreSQL connection is closed")
