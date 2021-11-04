from datetime import datetime
import pandas as pd
import pytz

bucketName = "gs://devel/"

# Get user data from Google Storage
def userData():
    users = pd.read_csv(bucketName+"users.csv")
    return users

# Format user's age
def ageUsers(users):
    ages = []
    birthdates = users["birthdate"]
    for birthdate in birthdates:
        this_year   = datetime.now().astimezone(pytz.timezone("Asia/Jakarta")).year
        date        = datetime.strptime(birthdate,"%Y-%m-%d")
        year        = datetime.strftime(date,"%Y")
        age         = int(this_year) - int(year)
        ages.append(age)
    
    ages_dataframe = pd.DataFrame(data=ages, columns=["age"])
    return ages_dataframe

# Format date
def dateFormat(dates, column):
    list_date = []
    for date in dates:
        date = datetime.strptime(date,"%Y-%m-%d %H:%M:%S")
        date_format = datetime.strftime(date, "%Y-%m-%d")
        list_date.append(date_format)
    dates = pd.DataFrame(data=list_date, columns=[column])
    return dates

# Send Data
def sendData(data):
    # Send to google storage for backup
    data.to_csv(bucketName+"result_users.csv",index=False)
    
    # Send to bigquery
    data.to_gbq(project_id="projectid", destination_table="users.user_data", if_exists="append")
        

def main():
    users = userData()
    username    = users["username"]
    created_at  = dateFormat(users["created_at"], "created_at")
    updated_at   = dateFormat(users["updated_at"], "updated_at")

    ages        = ageUsers(users)
    frames      = [username, created_at, updated_at, ages]

    data  = pd.concat(frames, axis=1)

    sendData(data)

if __name__ == "__main__":
    main()