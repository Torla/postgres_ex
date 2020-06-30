import psycopg2


def create_table(connection):
    cursor = connection.cursor()

    try:

        create_table_query = '''CREATE TABLE students
              (ID SERIAL   PRIMARY KEY ,
              NAME           TEXT    NOT NULL,
              SURNAME         TEXT NOT NULL,
              unique(NAME,SURNAME)); '''

        cursor.execute(create_table_query)

        create_table_query = '''CREATE TABLE courses
              (ID SERIAL  PRIMARY KEY ,
              NAME           TEXT    NOT NULL,
              DESCR        TEXT NOT NULL); '''

        cursor.execute(create_table_query)

        create_table_query = '''CREATE TABLE booking
              (STUDENT_ID BIGINT  ,
              COURSE_ID           BIGINT ,
              PRIMARY KEY (STUDENT_ID,COURSE_ID)
              ); '''

        cursor.execute(create_table_query)

        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error creating tables", error)
    finally:
        cursor.close()

def insert_names(connection):
    cursor = connection.cursor()

    try:

        nomi = [("Marco", "Torlaschi"), ("Roberta", "Latini"), ("Giorgo", "Franzini")]

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

def insert_booking(connection):
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
        cursor.execute("DROP TABLE  students  ")
        cursor.execute("DROP TABLE  courses  ")
        cursor.execute("DROP TABLE  booking  ")
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error deleting name", error)
    finally:
        cursor.close()


if __name__ == '__main__':
    print("esempi sql")



    try:
        connection = psycopg2.connect(user="postgres",
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="sample")


        #drop_tables(connection)

        create_table(connection)

        cursor = connection.cursor()

        insert_names(connection)

        insert_courses(connection)

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
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            connection.close()
            print("PostgreSQL connection is closed")
