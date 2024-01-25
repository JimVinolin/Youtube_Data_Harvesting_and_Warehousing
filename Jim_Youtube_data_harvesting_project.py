#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#Enter the Api key and connect:
def connect():
    api_key="AIzaSyCkj_7PKl7l41Gi-UPKTOCZ_eahUUR5FiU"
    api_service_name="youtube"    
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=api_key)
    
    return youtube
youtube=connect()

#Collect Channel info from the api channel part and create them as a function:
def channel_info(channel_id):
    request = youtube.channels().list(
        part ="snippet,contentDetails,Statistics",
        id=channel_id
    )
    responses=request.execute()
    
    for i in range(0,len(responses["items"])):
        data = dict(
                    Channel_Name = responses["items"][i]["snippet"]["title"],
                    Channel_Id = responses["items"][i]["id"],
                    Subscription_Count= responses["items"][i]["statistics"].get("subscriberCount"),
                    Views = responses["items"][i]["statistics"].get("viewCount"),
                    Total_Videos = responses["items"][i]["statistics"].get("videoCount"),
                    Channel_Description = responses["items"][i]["snippet"]["description"],
                    Playlist_Id = responses["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data

#Collect Channel_playlist videos from the api channels part and create them as a function:
def channel_playlist_videos(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                   part="contentDetails").execute()
    playlist_id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token=None
    while True:
        response1=youtube.playlistItems().list( 
                        part = 'snippet',
                        playlistId = playlist_id, 
                        maxResults = 50,
                        pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId']),
        next_page_token=response1.get('nextPageToken')    

        if next_page_token is None:
            break
    return video_ids

#Collect videos info from the api videos part and create them as a function:
def video_infos(video_ids):
    video_data=[]
    
    for video_id in video_ids:
        request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
                        id= video_id)
        response = request.execute()

        for item in response['items']:
            data=dict(Channel_Name = item['snippet']['channelTitle'],
                    Channel_Id = item['snippet']['channelId'],
                    Video_Id = item['id'],
                    Title = item['snippet']['title'],
                    Tags = item['snippet'].get('tags',[]),
                    Thumbnail = item['snippet']['thumbnails']['default']['url'],
                    Description = item['snippet']['description'],
                    Published_Date = item['snippet']['publishedAt'],
                    Duration = item['contentDetails']['duration'],
                    Views = item['statistics']['viewCount'],
                    Likes = item['statistics'].get('likeCount'),
                    Comments = item['statistics'].get('commentCount'),
                    Favorite_Count = item['statistics']['favoriteCount'],
                    Definition = item['contentDetails']['definition'],
                    Caption_Status = item['contentDetails']['caption'])
            video_data.append(data)
    return video_data  

#Collect Channel_playlist info from the api playlist part and create them as a function:
def channel_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data


#Collect comment info from the api commentthreads part and create them as a function:
#try is used for skip errors and run the programm.
def comment_infos(video_ids):
    Comment_info = []
    try:
        for video_id in video_ids:
                request = youtube.commentThreads().list(
                        part = "snippet",
                        videoId = video_id,
                        maxResults = 50
                        )
                response = request.execute()

                for item in response["items"]:
                        comment_infor = dict(
                                Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                Video_Id = item["snippet"]["videoId"],
                                Comment_Text = item["snippet"]["topLevelComment"]["snippet"].get("textOriginal"),
                                Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                        Comment_info.append(comment_infor)
    except:
        pass

    return Comment_info

    
#connection to Mongo db
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_datas"]

#Create a function to collect all channel informations in a single name:
def channel_information(channel_id):
    channel_details=channel_info(channel_id)
    playlist_details=channel_playlist_info(channel_id)
    video_ids=channel_playlist_videos(channel_id)
    video_details=video_infos(video_ids)
    comment_details=comment_infos(video_ids)
    
    collection=db["channel_information"]
    collection.insert_one({"channel_info":channel_details,"playlist_info":playlist_details,
                          "video_info":video_details,"comment_info":comment_details})
    
    return "upload completed"

# Create Channel table and insert the datas in PSSQL
def ch_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="PSSQL@987",
        database="youtube_datas",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = 'DROP TABLE IF EXISTS channels'
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''
            CREATE TABLE IF NOT EXISTS channels (
                Channel_Name VARCHAR(100),
                Channel_Id VARCHAR(80) PRIMARY KEY,
                Channel_Description TEXT,
                Subscribers BIGINT,
                Total_Videos INT,
                Views BIGINT,
                Playlist_Id VARCHAR(50)
            )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Channels Table alredy created")    

    channel_list = []
    db = client["Youtube_datas"]
    collection = db["channel_information"]
    for channel_data in collection.find({}, {"_id": 0, "channel_info": 1}):
        channel_list.append(channel_data["channel_info"])
    df = pd.DataFrame(channel_list)

    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO channels (
                Channel_Name,
                Channel_Id,
                Channel_Description,
                Subscribers,
                Total_Videos,
                Views,
                Playlist_Id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Channel_Description'],
            row['Subscription_Count'],
            row['Total_Videos'],
            row['Views'],
            row['Playlist_Id']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()

        except:
            print("Channels values are already inserted")

# Create Playlist table and insert the datas in PSSQL
def pl_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="PSSQL@987",
        database="youtube_datas",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = 'DROP TABLE IF EXISTS playlists'
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''
            CREATE TABLE IF NOT EXISTS playlists (
                PlaylistId VARCHAR(100) PRIMARY KEY,
                Title VARCHAR(100),
                ChannelId VARCHAR(100),
                ChannelName VARCHAR(100),
                PublishedAt TIMESTAMP,
                VideoCount INT
            )
        '''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("playlists Table alredy created")

    playlist_list = []
    db = client["Youtube_datas"]
    collection2 = db["channel_information"]
    for playlist_data in collection2.find({}, {"_id": 0, "playlist_info": 1}):
        for i in range(len(playlist_data["playlist_info"])):
            playlist_list.append(playlist_data["playlist_info"][i])
    df1 = pd.DataFrame(playlist_list)

    for index, row in df1.iterrows():
        insert_query = '''
            INSERT INTO playlists (
                PlaylistId,
                Title,
                ChannelId,
                ChannelName,
                PublishedAt,
                VideoCount
            ) VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['PlaylistId'],
            row['Title'],
            row['ChannelId'],
            row['ChannelName'],
            row['PublishedAt'],
            row['VideoCount']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Playlist values are already inserted.")

# Create Video table and insert the datas in PSSQL
def vid_table():    
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="PSSQL@987",
        database="youtube_datas",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = 'DROP TABLE IF EXISTS videos'
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''
            CREATE TABLE IF NOT EXISTS videos (
                Channel_Name VARCHAR(100),
                Channel_Id VARCHAR(100),
                Video_Id VARCHAR(50) PRIMARY KEY,
                Title VARCHAR(100),
                Tags TEXT,
                Thumbnail VARCHAR(250),
                Description TEXT,
                Published_Date TIMESTAMP,
                Duration INTERVAL,
                Views BIGINT,
                Likes BIGINT,
                Comments INT,
                Favorite_Count INT,
                Definition VARCHAR(25),
                Caption_Status VARCHAR(100)
            )
        '''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Video table already created")

    video_list = []
    db = client["Youtube_datas"]
    collection3 = db["channel_information"]
    for video_data in collection3.find({}, {"_id": 0, "video_info": 1}):
        for i in range(len(video_data["video_info"])):
            video_list.append(video_data["video_info"][i])
    df2 = pd.DataFrame(video_list)

    for index, row in df2.iterrows():
        insert_query = '''
            INSERT INTO videos (
                Channel_Name,
                Channel_Id,
                Video_Id,
                Title,
                Tags,
                Thumbnail,
                Description,
                Published_Date,
                Duration,
                Views,
                Likes,
                Comments,
                Favorite_Count,
                Definition,
                Caption_Status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Title'],
            row['Tags'],
            row['Thumbnail'],
            row['Description'],
            row['Published_Date'],
            row['Duration'],
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Favorite_Count'],
            row['Definition'],
            row['Caption_Status']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Video values are already inserted.")

# Create Comment table and insert the datas in PSSQL
def cmt_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="PSSQL@987",
        database="youtube_datas",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = 'DROP TABLE IF EXISTS comments'
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''
            CREATE TABLE IF NOT EXISTS comments (
                Comment_Id VARCHAR(255) PRIMARY KEY,
                Video_Id VARCHAR(255),
                Comment_Text TEXT,
                Comment_Author VARCHAR(255),
                Comment_Published DATETIME
            )
        '''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Comment table alredy created")

    comment_list = []
    db = client["Youtube_datas"]
    collection4 = db["channel_information"]
    for comment_data in collection4.find({}, {"_id": 0, "comment_info": 1}):
        for i in range(len(comment_data["comment_info"])):
            comment_list.append(comment_data["comment_info"][i])
    df3 = pd.DataFrame(comment_list)

    for index, row in df3.iterrows():
        insert_query = '''
            INSERT INTO comments (
                Comment_Id,
                Video_Id,
                Comment_Text,
                Comment_Author,
                Comment_Published
            ) VALUES (%s, %s, %s, %s, %s)
        '''
        values = (
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()

        except:
            print("Comment values alredy inserted")


# Call all tables in a single function
def tables():
    ch_table()
    pl_table()
    vid_table()
    cmt_table()
    
    return "Tables created"
    
#Streamlit channel,playlist,video,comment dataframe creation
def view_channel_table():
    channel_list = []
    db = client["Youtube_datas"]
    collection = db["channel_information"]
    for channel_data in collection.find({},{"_id":0,"channel_info":1}):
        channel_list.append(channel_data["channel_info"])
    ch_table=st.dataframe(channel_list)
    return ch_table

def view_playlist_table():   
    playlist_list=[]
    db=client["Youtube_datas"]
    collection2=db["channel_information"]
    for playlist_data in collection2.find({},{"_id":0,"playlist_info":1}):
        for i in range(len(playlist_data["playlist_info"])):
            playlist_list.append(playlist_data["playlist_info"][i])
    pl_table=st.dataframe(playlist_list)
    return pl_table

def view_video_table():    
    video_list=[]
    db=client["Youtube_datas"]
    collection3=db["channel_information"]
    for video_data in collection3.find({},{"_id":0,"video_info":1}):
        for i in range(len(video_data["video_info"])):
            video_list.append(video_data["video_info"][i])
    vid_table=st.dataframe(video_list)
    return vid_table

def view_comment_table():    
    comment_list=[]
    db=client["Youtube_datas"]
    collection4=db["channel_information"]
    for comment_data in collection4.find({},{"_id":0,"comment_info":1}):
        for i in range(len(comment_data["comment_info"])):
            comment_list.append(comment_data["comment_info"][i])
    cmt_table=st.dataframe(comment_list)
    return cmt_table

#streamlit UI Creation
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING by JIM]")
    st.header("SKILLS USED IN THIS PROJECT")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Check & store the data"):
    if channel_id:
        db = client["Youtube_datas"]
        collection = db["channel_information"]

        existing_channel = collection.find_one({"channel_info.Channel_Id": channel_id}, {"_id": 0, "channel_info": 1})

        if existing_channel:
            channel_info = existing_channel.get("channel_info")
            st.success(f"Channel ID: {channel_info['Channel_Id']} already exists.")
            st.dataframe([channel_info])
        else:
            insert = channel_information(channel_id)
            st.success(f"Channel ID {channel_id} inserted successfully.")

if st.button("Move data to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("view the selected table",(":blue[CHANNELS]",":yellow[PLAYLISTS]",":green[VIDEOS]",":black[COMMENTS]"))

if show_table==":blue[CHANNELS]":
    view_channel_table()

elif show_table==":yellow[PLAYLISTS]":
    view_playlist_table()

elif show_table==":green[VIDEOS]":
    view_video_table()

elif show_table==":black[COMMENTS]":
    view_comment_table()


#PSSQL connection and question part
mydb = psycopg2.connect(host="localhost",
               user="postgres",
               password="PSSQL@987",
               database= "youtube_datas",
               port = "5432"
               )
cursor = mydb.cursor()

questions=st.selectbox("Choose your questions",('1. All the videos and the Channel Name',
                             '2. Channels with most number of videos',
                             '3. 10 most viewed videos',
                             '4. Comments in each video',
                             '5. Videos with highest likes',
                             '6. likes of all videos',
                             '7. views of each channel',
                             '8. videos published in the year 2022',
                             '9. average duration of all videos in each channel',
                             '10. videos with highest number of comments'))

if questions == '1. All the videos and their Channel Name':
    query1 = "Select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    q1=cursor.fetchall()
    st.write(pd.DataFrame(q1, columns=["Video Title","Channel Name']))

elif questions == '2. All channel and thier number of videos":
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    q2=cursor.fetchall()
    st.write(pd.DataFrame(q2, columns=["Channel Name","No Of Videos"]))

elif questions == '3. Top 10 most viewed videos and thier channel name':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    q3 = cursor.fetchall()
    st.write(pd.DataFrame(q3, columns = ["views","channel Name","video title"]))

elif questions == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    q4=cursor.fetchall()
    st.write(pd.DataFrame(q4, columns=["No Of Comments", "Video Title"]))

elif questions == '5. Videos have the highest number of likes and thier channel name':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    q5 = cursor.fetchall()
    st.write(pd.DataFrame(q5, columns=["video Title","channel Name","like count"]))

elif questions == '6. Total number of likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    q6 = cursor.fetchall()
    st.write(pd.DataFrame(q6, columns=["like count","video title"]))

elif questions == '7. Total number of views for each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    q7=cursor.fetchall()
    st.write(pd.DataFrame(q7, columns=["channel name","total views"]))

elif questions == '8. Videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    q8=cursor.fetchall()
    st.write(pd.DataFrame(q8,columns=["Name", "Video Publised On", "ChannelName"]))

elif questions == '9. Average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    q9=cursor.fetchall()
    q9 = pd.DataFrame(q9, columns=['ChannelTitle', 'Average Duration'])
    Q9=[]
    for index, row in q9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        Q9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(Q9))

elif questions == '10. Videos with highest number of comments and thier channel name':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    q10=cursor.fetchall()
    st.write(pd.DataFrame(q10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))

