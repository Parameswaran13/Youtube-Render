import sqlite3
import pandas as pd
import streamlit as st

option = st.selectbox('Select a question',['1. What are the names of all the videos and their corresponding channels?','2.Which channels have the most number of videos, and how many videos do they have?','3. What are the top 10 most viewed videos and their respective channels?','4. How many comments were made on each video, and what are their corresponding video names?','5. Which videos have the highest number of likes, and what are their corresponding channel names?','6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?','7. What is the total number of views for each channel, and what are their corresponding channel names?','8. What are the names of all the channels that have published videos in the year 2022?','9. What is the average duration of all videos in each channel, and what are their corresponding channel names?','10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
if option == '1. What are the names of all the videos and their corresponding channels?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute(""" SELECT title AS video_title, channel_name as channel_name FROM Video ORDER BY channel_name DESC""")
    data1 = cur.fetchall()
    #for d in data1:
    column_names = [description[0] for description in cur.description]
    df1 = pd.DataFrame(data1,columns=column_names)
    st.write(df1)
    cur.close()
    connection.close()
        
elif option == '2.Which channels have the most number of videos, and how many videos do they have?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute("""SELECT channel_name as channel_name,total_videos as total_videos FROM Channel ORDER BY total_videos DESC""")
    data2 = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    df2 = pd.DataFrame(data2,columns=column_names)
    print(df2)
    st.bar_chart(df2)
    cur.close()
    connection.close()
    
elif option == '3. What are the top 10 most viewed videos and their respective channels?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute("""SELECT channel_name,title as video_title,views FROM Video ORDER BY views DESC LIMIT 10""")
    data3 = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    df3 = pd.DataFrame(data3,columns=column_names)
    print(df3)
    st.write(df3)
    cur.close()
    connection.close()

elif option == '4. How many comments were made on each video, and what are their corresponding video names?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute("""SELECT a.video_id as video_id,a.title as title,b.total_comments FROM Video AS a LEFT JOIN (SELECT video_id,COUNT(comment_id) AS total_comments FROM Comment GROUP BY video_id) AS b ON a.video_id = b.video_id ORDER BY b.total_comments DESC""")
    data4 = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    df4 = pd.DataFrame(data4,columns=column_names)
    print(df4)
    st.write(df4)
    cur.close()
    connection.close()

elif option == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute("""SELECT channel_name,title,likes AS likes_count FROM Video ORDER BY likes DESC LIMIT 10""")
    data5 = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    df5 = pd.DataFrame(data5,columns=column_names)
    print(df5)
    st.write(df5)
    cur.close()
    connection.close()

elif option == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute("""SELECT title,likes AS likes_count FROM Video ORDER BY likes DESC""")
    data6 = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    df6 = pd.DataFrame(data6,columns=column_names)
    print(df6)
    st.write(df6)
    cur.close()
    connection.close()

elif option == ' 7. What is the total number of views for each channel, and what are their corresponding channel names?':
    try:
        connection = sqlite3.connect('youtube1.db')
        cur=connection.cursor()
        cur.execute("""SELECT channel_name, views FROM Channel""")
        data7 = cur.fetchall()
        column_names = [description[0] for description in cur.description]
        df7 = pd.DataFrame(data7,columns=column_names)
        print(df7)
        st.write(df7)
        #cur.close()
    except sqlite3.Error as e:
        st.error(f"An error occured while wuerying the database:{str(e)}")
    finally:
        if connection:
            connection.close()
    
elif option == '8. What are the names of all the channels that have published videos in the year 2023?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute("""SELECT channel_name FROM Video WHERE published_date LIKE '2023%' GROUP BY channel_name ORDER BY channel_name""")
    data8 = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    df8 = pd.DataFrame(data8,columns=column_names)
    print(df8)
    st.write(df8)
    cur.close()
    connection.close()
    
elif option == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute("""SELECT channel_name, AVG(STRFTIME('?', duration) - STRFTIME('PTOS','PTOS')) /60 AS Average_Video_Duration FROM Video GROUP BY channel_name ORDER BY Average_Video_Duration DESC""")
    data9 = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    df9 = pd.DataFrame(data9,columns=column_names)
    print(df9)
    st.write(df9)
    cur.close()
    connection.close()


elif option == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    connection = sqlite3.connect('youtube1.db')
    cur=connection.cursor()
    cur.execute("""SELECT channel_name,video_id,comments FROM Video ORDER BY comments DESC LIMIT 10""")
    data10 = cur.fetchall()
    column_names = [description[0] for description in cur.description]
    df10 = pd.DataFrame(data10,columns=column_names)
    print(df10)  
    st.write(df10)
    cur.close()
    connection.close()