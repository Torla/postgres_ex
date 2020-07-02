import psycopg2

if __name__ == '__main__':
    conn = psycopg2.connect(user="postgres", password="password", host="127.0.0.1", port="5432", database="esercizio")

    cursor = conn.cursor()



    # tripID, duration, distance, dayOfWeek, Hour

    create_query = '''CREATE TABLE trips(
        ID TEXT PRIMARY KEY ,
        DURATION INT,
        DISTANCE DECIMAL ,
        DAY INT,
        HOUR INT
        )'''

    cursor.execute(create_query)

    insert = "INSERT INTO trips (ID,DURATION,DISTANCE,DAY,HOUR) VALUES (%s,%s,%s,%s,%s)"

    with open("data.csv") as file:
        file.readline()
        for line in file:
            data = line.split(",")
            id = data[0]
            duration = data[5]
            distance = data[6]
            day = data[11]
            hour = data[12]
            cursor.execute(insert, (id, duration, distance, day, hour))

    # day,hour durata media, distanza media (nuova tabella da creare)

    cursor.execute("SELECT HOUR,DAY,AVG(DISTANCE),AVG(DURATION) FROM trips GROUP BY HOUR,DAY")
    data = cursor.fetchall()

    cursor.execute('''CREATE TABLE trip_avg(
                    DAY INT,
                    HOUR INT,
                    DIST DECIMAL,
                    DURATION DECIMAL,
                    PRIMARY KEY(DAY,HOUR)
                    )''')

    for d in data:
        cursor.execute("INSERT INTO trip_avg(DAY,HOUR,DIST,DURATION) VALUES (%s,%s,%s,%s)", d)

    cursor.execute("SELECT * FROM trip_avg")

    for row in cursor.fetchall():
        print(row)

    cursor.execute("DELETE FROM trips")
    cursor.execute("DELETE FROM trip_avg")
    cursor.execute("DROP TABLE trip_avg")
    cursor.execute("DROP TABLE trips")

    conn.commit()

    conn.close()
