import streamlit as st
import certifi
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import pymongo
import sqlite3
import pandas as pd

#----------------------Connect MongoDB-----------------------------

client =  pymongo.MongoClient("mongodb://karthik:karthik@ac-6xfdclc-shard-00-00.mbwhehu.mongodb.net:27017,ac-6xfdclc-shard-00-01.mbwhehu.mongodb.net:27017,ac-6xfdclc-shard-00-02.mbwhehu.mongodb.net:27017/?ssl=true&replicaSet=atlas-q241bj-shard-0&authSource=admin&retryWrites=true&w=majority",tlsCAFile=certifi.where())
Mongodb=client.youtube1

#----------------------Connect sqlite3------------------------------

connection = sqlite3.connect("youtube1.db")

cursor = connection.cursor()

#---------------------Youtube API--------------------------

api_key = 'AIzaSyAU2IHMNwbqvMIv9kt7oYQCdDhdTOQos90'
youtube = build('youtube','v3',developerKey=api_key)
#channel_id = 'UC06rPr43Hz79M_jB_0OdlTQ'
#UCsc5d398QFJY5V1DlVjXbiw


#--------------Get Channel Details-----------------------
#ch_id =''
def get_channel_details(channel_id):

    ch_data = list()

    request = youtube.channels().list(part='contentDetails, snippet, statistics, status',
        id=channel_id)
    
    response = request.execute()

    for i in range(len(response['items'])):
        data = dict(channel_name=response['items'][i]['snippet']['title'],channel_id=response['items'][i]['id'],playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                  subscribers=response['items'][i]['statistics']['subscriberCount'],description=response['items'][i]['snippet']['description'],
                  views=response['items'][i]['statistics']['viewCount'],total_videos=response['items'][i]['statistics']['videoCount'])
        
        ch_data.append(data)

    return ch_data


#get_channel_details(ch_id)
#----------------------Create Channel Table------------------------------------------

command1 = """ CREATE TABLE IF NOT EXISTS Channel(\
    channel_name varchar(255),\
    channel_id varchar(255) PRIMARY KEY,\
    playlist_id varchar(255),\
    subscribers int,\
    description text,\
    views int,\
    total_videos int\
    )"""
cursor.execute(command1)

#--------------------------Get Video Id's------------------------------------------------
def get_channel_videos(channel_id):
  video_ids =list()
  next_page_token = None
  response = youtube.channels().list(id=channel_id,part='contentDetails').execute()
  playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  while True:
    res = youtube.playlistItems().list(playlistId=playlist_id,part='snippet,contentDetails').execute()
    for i in range(len(res['items'])):
      video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token = res.get('nextPageToken')

    if next_page_token is None:
        break
  return video_ids

#------------------Get Video Stats-------------------------------------------------

def get_video_details(v_ids):
    video_stats = []

    for i in range(0, len(v_ids), 1):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(channel_name=video['snippet']['channelTitle'],
                                 channel_id=video['snippet']['channelId'],
                                 video_id=video['id'],
                                 title=video['snippet']['title'],
                                 thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 description=video['snippet']['description'],
                                 published_date=video['snippet']['publishedAt'],
                                 duration=video['contentDetails']['duration'],
                                 views=video['statistics']['viewCount'],
                                 likes=video['statistics'].get('likeCount'),
                                 comments=video['statistics'].get('commentCount'),
                                 favorite_count=video['statistics']['favoriteCount'],
                                 definition=video['contentDetails']['definition'],
                                 caption_status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats
#------------------Create Video Table---------------------------
command2 = """ CREATE TABLE IF NOT EXISTS Video(\
    channel_name varchar(255),channel_id varchar(255),\
    video_id varchar(255) PRIMARY KEY,title varchar(255),\
    thumbnail varchar(255),description text,\
    published_date varchar(255),duration varchar(255),\
    views int,likes int,comments int,favorite_count int,\
    definition varchar(255),caption_status varchar(255),\
    FOREIGN KEY (channel_id) REFERENCES Channel(channel_id)
    ) """

cursor.execute(command2)

#----------------- Get Comments details-------------------------------------

def get_comments_details(v_id):
  comment_data = []
  try:



    next_page_token = None
    while True:

        response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=100,
                                                     pageToken=next_page_token).execute()
        for cmt in response['items']:
          data = dict(comment_id=cmt['id'],
                            video_id=cmt['snippet']['videoId'],
                            comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            reply_count=cmt['snippet']['totalReplyCount']
                            )


          comment_data.append(data)
          next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break

  except:
      pass

  return comment_data
  
#------------------Create table for comment----------------------------------------

command3= """ CREATE TABLE IF NOT EXISTS Comment(\
    comment_id varchar(255) PRIMARY KEY,video_id varchar(255),\
    comment_text text,comment_author varchar(255),\
    comment_posted_date varchar(255),like_count int,\
    reply_count int,FOREIGN KEY(video_id) REFERENCES Video(video_id))"""
cursor.execute(command3)

#----------------------Extract channel names from Mongodb---------------------------------


def channel_names():
    ch_names = []
    for i in Mongodb.channel_details.find({},{'_id':0}):
        ch_names.append(i['channel_name'])
        print(ch_names)
    return ch_names
 
channel_names()

#--------------------------------Streamlit design-----------------------------------

#with st.sidebar:
 #   option = option_menu(None,["Home"],icons=['house'],menu_icon="cast",default_index=0,orientation="horizontal")

#if option == "Home":
st.title('Youtube Data Harvesting and Warehousing')
st.write('This approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.')
tab1, tab2, tab3 = st.tabs(["Get Data","Load to MongoDB","Transform to SQL"])
#ch_id =''
with tab1:
    st.header("Get your youtube channel data")
    ch_id = st.text_input('Enter Channel ID')

    if ch_id and st.button('Submit'):
        c_deatils = get_channel_details(ch_id)
        st.json(c_deatils)
        st.success('Successfully displayed data')


with tab2:
    st.header("Upload to MongoDB")
    if st.button('Upload to MongoDB'):
        c_deatils = get_channel_details(ch_id)
        v_ids = get_channel_videos(ch_id)
        video_stats = get_video_details(v_ids)
        
        def comments():
            cm_d =[]
            for i in v_ids:
                cm_d +=  get_comments_details(i)
            return cm_d
        
        com_details = comments()

         
        #create collection in Mongodb and insert data to Mongodb
        collection1 = Mongodb.channel_details
        collection1.insert_many(c_deatils)
        collection2 = Mongodb.video_details
        collection2.insert_many(video_stats)
        collection3 = Mongodb.comment_details
        collection3.insert_many(com_details)
        st.success('Upload Mongodb successfull')
        st.balloons()
        

with tab3:
    st.header("Load it to SQL")
    channel_name_list = channel_names()
    user_input=st.selectbox('Select an item',channel_name_list)
        #user_input_df = pd.DataFrame(user_input)
        #st.selectbox('options',options=('Car','Bike'))
    st.write("Selected channel is:",user_input)
        #st.cache(allow_output_mutation=True)
      
      #--------------------Load to SQL--------------------

    def insert_into_Channel(user_input):
        collection1 = Mongodb.channel_details
        
        try:
            
            connection = sqlite3.connect('youtube1.db')
            cursor = connection.cursor()
                #collection1.insert_many(c_details)
                    
            query1 =  '''INSERT INTO Channel(channel_name,channel_id,playlist_id,subscribers,description,views,total_videos) VALUES (?,?,?,?,?,?,?) '''

                    #channel_df = [i for i in collection1.find({"Channel_name": user_input}, {'_id': 0})]
            for i in collection1.find({'channel_name':user_input},{'_id':0}):
                value1 = tuple(i.values())
                cursor.execute(query1, value1)
            connection.commit()
            cursor.close()
            connection.close()
            #print("Data inserted successfully")

        except sqlite3.Error as e:
                print(f"SQLite error: {e}")
        except Exception as e:
                print(f"Error: {e}")

        return cursor.lastrowid
    
    def insert_into_video(user_input):
        collection2 = Mongodb.video_details
        try:
            connection = sqlite3.connect('youtube1.db')
            cursor = connection.cursor()
            
            query2 = ''' INSERT INTO Video(channel_name,channel_id,video_id,title,thumbnail,description,published_date,duration,views,likes,comments,favorite_count,definition,caption_status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            for i in collection2.find({'channel_name':user_input},{'_id':0}):
                value2 = tuple(i.values())
                cursor.execute(query2, value2)
            connection.commit()
            cursor.close()
            connection.close()
        except sqlite3.Error as e:
            print(f"SQLite error:{e}")
        except Exception as e:
            print(f"Error: {e}")
        return cursor.lastrowid
    
    def insert_into_comment(user_input):
        collection2 = Mongodb.video_details
        collection3 = Mongodb.comment_details
        try:
            connection = sqlite3.connect('youtube1.db')
            cursor = connection.cursor()
            
            query3 = ''' INSERT INTO Comment(comment_id,video_id,comment_text,comment_author,comment_posted_date,like_count,reply_count) VALUES(?,?,?,?,?,?,?)'''
            for vid in collection2.find({'channel_name':user_input},{'_id':0}):
                for i in collection3.find({'video_id':vid['video_id']},{'_id':0}):
                    value3 = tuple(i.values())
                    cursor.execute(query3, value3)
            connection.commit()
            cursor.close()
            connection.close()
        except sqlite3.Error as e:
            print(f"SQLite error:{e}")
        except Exception as e:
            print(f"Error: {e}")
        return cursor.lastrowid
                
        
                    
                    
    if st.button("Transform to SQL"):
        
        try:
            insert_into_Channel(user_input)
            insert_into_video(user_input)
            insert_into_comment(user_input)
            st.balloons()
            st.success("Data inserted successfully")
        except:
            st.error("Channel already transformed!")

