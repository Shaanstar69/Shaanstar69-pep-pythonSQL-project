import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():
    
    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users(conn, 'resources/users.csv')
    load_and_clean_call_logs(conn, 'resources/callLogs.csv')
    write_user_analytics(conn,'resources/userAnalytics.csv')
    write_ordered_calls(conn,'resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()
    
    def return_cursor():
        return cursor

# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(filepath="resources/users.csv"):
    cursor = return_cursor()

    with open(filepath, mode='r', newline='') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)  

        for row in reader:
            if len(row) != 2:
                continue

            first_name, last_name = row[0].strip(), row[1].strip()

            if not first_name or not last_name:
                continue

            cursor.execute("INSERT INTO users (firstName, lastName) VALUES (?, ?)",(first_name, last_name))

    conn.commit()
    #print("TODO: load_users")


# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(filepath="resources/callLogs.csv"):
    cursor = return_cursor()

    with open(filepath, mode='r', newline='') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)  

        for row in reader:
            if len(row) != 5:
                continue

            phone, start, end, call_type, user_id = [col.strip() for col in row]

            
            if not phone or not start or not end or not call_type or not user_id:
                continue

            try:
                start = int(start)
                end = int(end)
                user_id = int(user_id)
            except ValueError:
                continue

            
            cursor.execute(
                "INSERT INTO callLogs (phoneNumber, startTime, endTime, direction, userId) VALUES (?, ?, ?, ?, ?)",
                (phone, start, end, call_type, user_id)
            )

    
    conn.commit()

    #print("TODO: load_call_logs")


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    cur = return_cursor()

    cur.execute('''
        SELECT userId, ROUND(AVG(endTime - startTime), 1) AS avgDuration, COUNT(*) AS numCalls
        FROM callLogs
        GROUP BY userId
    ''')

    rows = cur.fetchall()

    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])  
        for userId, avgDuration, numCalls in rows:
            writer.writerow([userId, avgDuration, numCalls])

    #print("TODO: write_user_analytics")


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    cur = return_cursor()

    cur.execute('''
        SELECT phoneNumber, startTime, endTime, direction, userId
        FROM callLogs
        ORDER BY userId, startTime
    ''')
    ordered_calls = cur.fetchall()

    with open(csv_file_path, mode='w', newline='') as file:

        writer = csv.writer(file)
        writer.writerow(['rowNumber', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])

        for i, row in enumerate(ordered_calls, start=1):
            writer.writerow([i] + list(row))

    #print("TODO: write_ordered_calls")



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
