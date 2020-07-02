import psycopg2


def init():
    create_table_query = '''CREATE TABLE trips
                  (ID TEXT   PRIMARY KEY ,
                  DURATION   INT        ,
                  DISTANCE   DECIMAL,  
                  DAY INT,
                  HOUR INT); '''
    cursor.execute(create_table_query)
    connection.commit()
    insert = "INSERT INTO trips (ID,DURATION,DISTANCE,DAY,HOUR) VALUES (%s,%s,%s,%s,%s)"
    with open("data.csv") as file:
        file.readline()
        for line in file:
            lista = line.split(sep=",")
            cursor.execute(insert, (lista[0], lista[5], lista[6], lista[11], lista[12]))
    connection.commit()


if __name__ == '__main__':

    try:

        connection = psycopg2.connect(user="postgres",
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="sample2")

        cursor = connection.cursor()

        #init()

        #cursor.execute("SELECT DAY,AVG(DURATION) FROM trips GROUP BY DAY ")

        for line in cursor.fetchall():
            print(line)





    except (Exception, psycopg2.Error) as error:
        # catcho l'errore in connessione
        print("Error while connecting to PostgreSQL", error)
    finally:
        # chiudo la connessione in finally per essere sicuro che avvenga sempre
        if (connection):
            connection.close()
            print("PostgreSQL connection is closed")
