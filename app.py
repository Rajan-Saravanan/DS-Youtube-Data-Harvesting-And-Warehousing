import streamlit as st
import pandas as pd
import googleapiclient.discovery
import googleapiclient.errors
import pymongo
import certifi
import sqlite3
from streamlit_option_menu import option_menu
import datetime

# Account 1
# api_sec_key = "AIzaSyANq-DIAtHDz4N0PmS3DdGf5prav1YjhGg"
# Account 2
# api_sec_key = "AIzaSyDgBH0MTQNerNeV6zeCfa3A5wcoOWglhBA"
# Account 3
api_sec_key = "AIzaSyDGnjOmsBs8i2cE30gHT42vlUW2j3a8Lx4"

class AppController:
    # Streamlit App Navigaion controller
    def controller(self):
        if 'gathered_channel_data' not in st.session_state:
            st.session_state.gathered_channel_data = {}
        
        with st.sidebar:
            selected  = option_menu(
                menu_title= None,
                options=['Extract Data', 'Transform Data', 'Analysis'],
                icons=['database-fill-down', 'circle-square', 'signal'],
                default_index=0
            )

        if selected == 'Extract Data':
            extract_data = ExtractData()
            extract_data.display_page()
        if selected == 'Transform Data':
            transform_data = TransformData()
            transform_data.display_page()
        if selected == 'Analysis':
            analysis = Analysis()
            analysis.display_page()

class ExtractData:
    def display_page(self):
        st.title("Extract Data From Youtube")
        self.__ui_element_to_get_channel_id()
    
    def __ui_element_to_get_channel_id(self):
        col1, col2 = st.columns(2)
        channel_id = col1.text_input("Enter Youtube Channel ID")
        api_token = col2.text_input("Enter API Key")
        # st.button("Get Channel Data", on_click=self.__validate_and_get_channel_data, args=[channel_id, api_token], use_container_width=True)
        if st.button("Extract Channel Data From Youtube", use_container_width=True):
            self.__validate_and_get_channel_data(channel_id, api_token)

            
    def __validate_and_get_channel_data(self, channel_id, api_key):
        if channel_id and api_key:
            st.session_state.gathered_channel_data = self.__get_channel_data(channel_id, api_key)
            channel_data = st.session_state.gathered_channel_data
            if channel_data:
                channel_name = channel_data['channel']['channel_name']
                st.success(f"{channel_name} Channel Data has been gathered")
                st.write(channel_data['channel'])
                self.__save_channel_data_to_mongo_db(channel_name,channel_data)       
        else:
            st.error("Please Enter Channel ID and API Key")

    def __get_channel_data(self, channel_id, api_token):
        youtube = YoutubeUtil(api_token)
        return youtube.get_channel_details(channel_id)
    
    def __save_channel_data_to_mongo_db(self, channel_name, channel_data):
        mongoDB = MongoDB()
        if mongoDB.is_channel_exist_in_db(channel_name) is None:
            st.button("Save Channel Data in MongoDB", on_click=self.__insert_channel_render, args=[mongoDB, channel_data],use_container_width=True)
        else:
            st.info("Channel Data is already available in MongoDB. Do you want to Update Channel Data?")
            st.button("Update Channel Data in MongoDB", on_click=self.__update_channel_render, args=[mongoDB, channel_data], use_container_width=True)
    
    def __insert_channel_render(self, mongoDB, channel_data):
        inserted = mongoDB.insert_channel_document(channel_data)
        if inserted.inserted_id:
            st.success(f"{channel_data['channel']['channel_name']} Channel Data has been inserted to MongoDB")
        else:
            st.error(f"{channel_data['channel']['channel_name']} Channel Data failed to insert to MongoDB")
        st.session_state.gathered_channel_data = {}

    def __update_channel_render(self, mongoDB, channel_data):
        update = mongoDB.update_channel_document(channel_data)
        if update.inserted_id:
            st.success(f"{channel_data['channel']['channel_name']} Channel Data has been Updated to MongoDB")
        else:
            st.error(f"{channel_data['channel']['channel_name']} Channel Data failed to update to MongoDB")
        st.session_state.gathered_channel_data = {}

class TransformData:
    def display_page(self):
        st.title("Transform Data From MongoDB to SQL")
        self.__ui_element_to_get_transform_data()
        self.sql = SQL()
    
    def __ui_element_to_get_transform_data(self):
        sql = SQL()
        mongo = MongoDB()
        channels_in_sql = sql.get_channels_in_db()
        channels_mongo = mongo.get_list_of_channels_in_db()
        pending_to_transform = [channel for channel in channels_mongo if channel not in channels_in_sql]
        channels_in_both_db = [channel for channel in channels_mongo if channel in channels_in_sql]
        print(f"Pending to Transform {len(pending_to_transform)}")
        print(f"Channels in Both DB {len(channels_in_both_db)}")
        delta_transform_pending = self.__transform_delta_update_pending(mongo, sql, channels_in_both_db)
        if pending_to_transform:
            st.header("Channel Data Pending for Transform")
            channel_selected = self.__dropdown_to_transform_data(pending_to_transform)
            st.button(label="Transform Data to SQL", use_container_width=True, on_click=self.__transform_data_to_sql, args=[mongo, channel_selected])
        elif delta_transform_pending:
            st.header("Channel Data Pending for Delta Transform")
            delt_update_channel = self.__dropdown_pending_to_transform_data(delta_transform_pending)
            st.button(label="Transform Delta Data to SQL", use_container_width=True, on_click=self.__transform_delta_data_to_sql, args=[mongo, delt_update_channel])
        else:
            st.warning("Please Perform 'Extract Data' to continue with Transform Data")
        sql.close_connection()
    
    def __dropdown_to_transform_data(self, pending_to_transform):
        transform = st.selectbox(
            label = "Select a Channel to Transform Data to SQL from MongoDB",
            options = pending_to_transform,
            index = None,
            placeholder="Choose an option"
        )
        return transform
    
    def __dropdown_pending_to_transform_data(self, delta_pending_to_transform):
        transform = st.selectbox(
            label = "Select a Channel to Transform Delta Pending Data to SQL from MongoDB",
            options = delta_pending_to_transform,
            index = None,
            placeholder="Choose an option"
        )
        return transform
    
    def __transform_delta_update_pending(self, mongo, sql, channels_in_both_db):
        delta_pending = list()
        for channel in channels_in_both_db:
            m_channel = mongo.is_channel_exist_in_db(channel)
            data_extracted_at = m_channel['data_extracted_at']
            print(f"{channel} extracted at {data_extracted_at}")
            sql.cursor.execute("SELECT channel_name FROM channel WHERE channel_name = ? AND data_extracted_at != ? LIMIT 1", (channel, data_extracted_at))
            s_res = sql.cursor.fetchall()
            for rec in s_res:
                delta_pending.append(rec[0])
                print(f"Delta Pending channel {rec[0]}")
        return delta_pending
    
    def __transform_data_to_sql(self, mongo, channel_name):
        sql = SQL()
        m_channel = mongo.get_channel_document(channel_name)
        self.__transform_channel_obj_to_channel_table(sql, m_channel['channel'])
        self.__transform_playlist_obj_to_playlist_table(sql, m_channel)
        self.__transform_video_obj_to_video_table(sql, m_channel)
        sql.commit_changes()
        sql.close_connection()

    def __transform_delta_data_to_sql(self, mongo, channel_name):
        sql = SQL()
        channel_id = sql.get_channel_id_from_db(channel_name)
        self.__delete_channel_and_associated_records(sql, channel_id[0])
        self.__transform_data_to_sql(mongo, channel_name)
        sql.close_connection()

    def __transform_channel_obj_to_channel_table(self, sql, channel_obj):
        channel_data = (
            channel_obj['channel_id'],
            channel_obj['channel_name'],
            channel_obj['subscription_count'],
            channel_obj['channel_views'],
            channel_obj['channel_description'],
            channel_obj['upload_id'],
            channel_obj['data_extracted_at']
        )
        sql.cursor.execute('''
            INSERT INTO channel (channel_id, channel_name, subscription_count, channel_views, channel_description, upload_id, data_extracted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', channel_data)
        st.toast(body="Channel Data has been Transformed", icon='🪄')

    def __transform_playlist_obj_to_playlist_table(self, sql, channel_obj):
        for playlist_data in channel_obj['playlists']:
            playlist_data_tuple = (
                playlist_data['playlist_id'],
                playlist_data['playlist_name'],
                playlist_data['channel_id']
            )
            sql.cursor.execute('''
                INSERT INTO playlist (playlist_id, playlist_name, channel_id)
                VALUES (?, ?, ?)
            ''', playlist_data_tuple)
        st.toast(body="Playlist Data has been Transformed", icon='🪄')

    def __transform_video_obj_to_video_table(self, sql, channel_obj):
        for video_data in channel_obj['videos']:
            video_id = video_data['video_id']
            playlist_id = video_data['playlist_id']

            video_data_tuple = (
                video_id,
                playlist_id,
                video_data['video_name'],
                video_data['video_description'],
                video_data['published_date_time'],
                video_data['view_count'],
                video_data['like_count'],
                video_data['favourite_count'],
                video_data['comment_count'],
                video_data['duration'],
                video_data['thumbnail'],
                video_data['caption_status']
            )

            sql.cursor.execute('''
                INSERT OR IGNORE INTO video (video_id, playlist_id, video_name, video_description, published_at, view_count,
                                like_count, favourite_count, comment_count, duration, thumbnail, caption_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', video_data_tuple)

            for comment_data in video_data['comments']:
                comment_data_tuple = (
                    comment_data['comment_id'],
                    comment_data['comment_text'],
                    comment_data['comment_author'],
                    comment_data['comment_published_date_time'],
                    video_id
                )

                sql.cursor.execute('''
                    INSERT OR IGNORE INTO comment (comment_id, comment_text, comment_author, comment_published_date_time, video_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', comment_data_tuple)
        st.toast(body="Video and Comment data has been Transformed", icon='🪄')

    def __delete_channel_and_associated_records(self, sql, channel_id):
        try:
            # Disable foreign key constraints temporarily for manual cascading delete
            sql.cursor.execute('PRAGMA foreign_keys=OFF')

            # Delete comments associated with videos from the specified channel
            sql.cursor.execute('''
                DELETE FROM comment
                WHERE video_id IN (
                    SELECT video_id FROM video WHERE playlist_id IN (
                        SELECT playlist_id FROM playlist WHERE channel_id=?
                    )
                )
            ''', (channel_id,))

            # Delete videos associated with playlists from the specified channel
            sql.cursor.execute('''
                DELETE FROM video
                WHERE playlist_id IN (
                    SELECT playlist_id FROM playlist WHERE channel_id=?
                )
            ''', (channel_id,))

            # Delete playlists associated with the specified channel
            sql.cursor.execute('DELETE FROM playlist WHERE channel_id=?', (channel_id,))

            # Finally, delete the channel itself
            sql.cursor.execute('DELETE FROM channel WHERE channel_id=?', (channel_id,))

            # Commit the changes
            sql.commit_changes()
            st.toast(body=f"Channel {channel_id} and associated records deleted successfully.", icon='🗑️')

        finally:
            # Re-enable foreign key constraints
            sql.cursor.execute('PRAGMA foreign_keys=ON')

class Analysis:

    def __init__(self):
        self.__sql = SQL()

    def display_page(self):
        st.title("Youtube Transformed data analysis")
        self.__display_analysis_option()
    
    def __display_analysis_option(self):
        a1 = "What are the names of all the videos and their corresponding channels?"
        a2 = "Which channels have the most number of videos, and how many videos do they have?"
        a3 = "What are the top 10 most viewed videos and their respective channels?"
        a4 = "How many comments were made on each video, and what are their corresponding video names?"
        a5 = "Which videos have the highest number of likes, and what are their corresponding channel names?"
        a6 = "What is the total number of likes and dislikes for each video, and what are their corresponding video names?"
        a7 = "What is the total number of views for each channel, and what are their corresponding channel names?"
        a8 = "What are the names of all the channels that have published videos in the year 2022?"
        a9 = "What is the average duration of all videos in each channel, and what are their corresponding channel names?"
        a10 = "Which videos have the highest number of comments, and what are their corresponding channel names?"
        list_of_options = [a1, a2, a3, a4, a5, a6, a7, a8, a9, a10]
        selected_analysis = st.selectbox(label="Select an option to view output of the Analysis", options=list_of_options, placeholder="Select an option", index=None)

        if selected_analysis == a1:
            return self.__analysis_1()
        elif selected_analysis == a2:
            return self.__analysis_2()
        elif selected_analysis == a3:
            return self.__analysis_3()
        elif selected_analysis == a4:
            return self.__analysis_4()
        elif selected_analysis == a5:
            return self.__analysis_5()
        elif selected_analysis == a6:
            return self.__analysis_6()
        elif selected_analysis == a7:
            return self.__analysis_7()
        elif selected_analysis == a8:
            return self.__analysis_8()
        elif selected_analysis == a9:
            return self.__analysis_9()
        elif selected_analysis == a10:
            return self.__analysis_10()
    
    def __analysis_1(self):
        sql = '''SELECT v.video_name as 'Video Name', c.channel_name as 'Channel Name' from video as v LEFT JOIN playlist as p on v.playlist_id = p.playlist_id LEFT JOIN channel as c on p.channel_id = c.channel_id ORDER BY c.channel_name ASC;'''
        df = pd.read_sql_query(con=self.__sql.get_sql_connection(), sql=sql)
        st.dataframe(df)
    
    def __analysis_2(self):
        sql = '''SELECT c.channel_name as 'Channel Name', COUNT(v.video_id) as 'Video Count' FROM video as v LEFT JOIN playlist as p ON v.playlist_id = p.playlist_id LEFT JOIN channel as c on c.channel_id = p.channel_id GROUP BY c.channel_name ORDER BY COUNT(v.video_id) DESC;'''
        df = pd.read_sql_query(con=self.__sql.get_sql_connection(), sql=sql)
        st.dataframe(df)
    
    def __analysis_3(self):
        channels = self.__sql.get_channels_in_db()
        selected_channel = st.selectbox(label="Select a Channel to View Top 10 'View Count - Video'", options=channels, index=None, placeholder="Select a channel")
        if selected_channel:
            sql = f'''SELECT v.video_name as 'Video Name', v.view_count as 'Video View Count' FROM video as v LEFT JOIN playlist as p ON v.playlist_id = p.playlist_id LEFT JOIN channel as c on c.channel_id = p.channel_id WHERE c.channel_name = '{selected_channel}' ORDER BY v.view_count DESC LIMIT 10 ;'''
            df = pd.read_sql_query(con=self.__sql.get_sql_connection(), sql=sql)
            st.dataframe(df)
    
    def __analysis_4(self):
        channels = self.__sql.get_channels_in_db()
        selected_channel = st.selectbox(label="Select a Channel to View Top 10 'Comment Count of Video'", options=channels, index=None, placeholder="Select a channel")
        if selected_channel:
            sql = f'''SELECT v.video_name as 'Video Name', v.comment_count as 'Video Comment Count' FROM video as v LEFT JOIN playlist as p ON v.playlist_id = p.playlist_id LEFT JOIN channel as c on c.channel_id = p.channel_id WHERE c.channel_name = '{selected_channel}' ORDER BY v.comment_count DESC ;'''
            df = pd.read_sql_query(con=self.__sql.get_sql_connection(), sql=sql)
            st.dataframe(df)

    def __analysis_5(self):
        sql = '''SELECT v.video_name as 'Video Name', v.like_count as 'Video Like Count', c.channel_name as 'Channel Name' FROM video as v LEFT JOIN playlist as p ON v.playlist_id = p.playlist_id LEFT JOIN channel as c on c.channel_id = p.channel_id ORDER BY v.like_count DESC;'''
        df = pd.read_sql_query(con=self.__sql.get_sql_connection(), sql=sql)
        st.dataframe(df)
    
    def __analysis_6(self):
        sql = '''SELECT v.video_name as 'Video Name', v.like_count as 'Video Like Count', c.channel_name as 'Channel Name' FROM video as v LEFT JOIN playlist as p ON v.playlist_id = p.playlist_id LEFT JOIN channel as c on c.channel_id = p.channel_id'''
        df = pd.read_sql_query(con=self.__sql.get_sql_connection(), sql=sql)
        st.dataframe(df)
    
    def __analysis_7(self):
        sql = '''SELECT channel_name AS 'Channel Name', channel_views AS 'Channel View Count' FROM channel ORDER BY channel_views DESC;'''
        df = pd.read_sql_query(con=self.__sql.get_sql_connection(), sql=sql)
        st.dataframe(df)

    def __analysis_8(self):
        sql = '''SELECT DISTINCT c.channel_name AS 'Channel Name' FROM channel AS c LEFT JOIN playlist as p on c.channel_id = p.channel_id LEFT JOIN video as v on v.playlist_id = p.playlist_id WHERE v.published_at >= '2022-01-01' and v.published_at <= '2022-12-30';'''
        df = pd.read_sql_query(con=self.__sql.get_sql_connection(), sql=sql)
        st.dataframe(df)
    
    def __analysis_9(self):
        sql = '''SELECT TIME(AVG(v.duration), 'unixepoch') AS 'Average Duration', c.channel_name AS 'Channel Name' FROM video AS v LEFT JOIN playlist as p on v.playlist_id = p.playlist_id LEFT JOIN channel as c on c.channel_id = p.channel_id GROUP BY c.channel_name;'''
        df = pd.read_sql_query(con = self.__sql.get_sql_connection(), sql=sql)
        st.dataframe(df)

    def __analysis_10(self):
        sql = '''SELECT v.video_name AS 'Video Name', v.comment_count AS 'Video Comment Count', c.channel_name AS 'Channel Name' FROM video AS v LEFT JOIN playlist as p on v.playlist_id = p.playlist_id LEFT JOIN channel as c on c.channel_id = p.channel_id ORDER BY v.comment_count DESC;'''
        df = pd.read_sql_query(con = self.__sql.get_sql_connection(), sql=sql)
        st.dataframe(df)

class YoutubeUtil:
    def __init__(self, api_key):
        api_service_name = "youtube"
        api_version = "v3"
        self.__youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

    def __extract_channel_data(self, response):
        return {
                "channel_name" : response['items'][0]['snippet']['title'],
                "channel_id" : response['items'][0]['id'],
                "subscription_count" : response['items'][0]['statistics']['subscriberCount'],
                "channel_views" : response['items'][0]['statistics']['viewCount'],
                "channel_description" : response['items'][0]['snippet']['description'],
                "upload_id" : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                "data_extracted_at" : datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp()).isoformat()
            }
    def __extract_playlist_data(self, playlist_items):
        data = []
        for playlist in playlist_items:
            data.append({
                "playlist_id" : playlist['id'],
                "channel_id" : playlist['snippet']['channelId'],
                "playlist_name" : playlist['snippet']['title']
            })
        return data
    
    def __extract_video_id_data(self, playlist_items, playlist_id):
        video_ids = []
        for playlist in playlist_items:
            video_ids.append(playlist['contentDetails']['videoId'])
        video_data = self.__get_video_data(video_ids, playlist_id)
        return video_data
    
    def __extract_video_data(self, video_data, playlist_id):
        playlist_videos = list()
        for video in video_data:
            caption = {'true': 'Available', 'false': 'Not Available'}
            video_info =  {
                'video_id': video['id'],
                'playlist_id' : playlist_id,
                'video_name': video['snippet']['title'],
                'video_description': video['snippet']['description'],
                'tags': video['snippet'].get('tags', []),
                'published_date_time': video['snippet']['publishedAt'],
                'view_count': video['statistics']['viewCount'],
                'like_count': video['statistics'].get('likeCount', 0),
                'favourite_count': video['statistics']['favoriteCount'],
                'comment_count': video['statistics'].get('commentCount', 0),
                'duration': pd.to_timedelta(video['contentDetails']['duration']).total_seconds(),
                'thumbnail': video['snippet']['thumbnails']['default']['url'],
                'caption_status': caption[video['contentDetails']['caption']],
            }
            playlist_videos.append(video_info)
        return playlist_videos

    def __extract_comment_data(self, video_comments, video_id):
        comments = list()
        for comment in video_comments:
            data = {'comment_id': comment['id'],
                    'video_id' : video_id,
                    'comment_text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_published_date_time': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
            comments.append(data)
        return comments
    
    def __get_video_comments(self, video_id):
        request = self.__youtube.commentThreads().list(
            part='id, snippet',
            videoId=video_id,
            maxResults=100
        )
        try:
            resp = request.execute()
        except googleapiclient.errors.HttpError:
            return []
        data = self.__extract_comment_data(resp['items'], video_id)
        nextPage = resp.get('nextPageToken')
        while nextPage is not None:
            request = self.__youtube.commentThreads().list(
                part='id, snippet',
                videoId=video_id,
                maxResults=100,
                pageToken=nextPage
            )
            try:
                resp = request.execute()
            except googleapiclient.errors.HttpError:
                return []
            data.extend(self.__extract_comment_data(resp['items'], video_id))
            nextPage = resp.get('nextPageToken')
        return data

    def __get_video_data(self, video_ids, playlist_id):
        pending_video = len(video_ids)
        starts_at = 0
        maxResult = 50
        output = list()
        print(f"Collecting Video Data for {len(video_ids)} from playlist {playlist_id}")
        while pending_video > 0:
            if len(video_ids) < maxResult:
                ids = ','.join(video_ids)
                pending_video -= len(video_ids)
            else:
                ends_at = starts_at+maxResult
                if ends_at > len(video_ids):
                    excess = ends_at - len(video_ids)
                    gap = maxResult - excess
                    ends_at = starts_at + gap
                ids = ','.join(video_ids[starts_at: ends_at])
                starts_at = ends_at
                pending_video -= maxResult
            request = self.__youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=ids,
                maxResults = maxResult
            )
            resp = request.execute()
            output.extend(resp['items'])
            print(f"Collected Video Data for {len(video_ids)} from playlist {playlist_id}")
        return self.__extract_video_data(output, playlist_id)
    
    def __get_comment_data(self, videos_list):
        comments = list()
        for video in videos_list:
            comments.extend(self.__get_video_comments(video['video_id']))
        return comments

    def __get_video_data_from_playlist_with_comments(self, playlists):
        collection_of_video_id = list()
        for playlist in playlists:
            playlist_video = list()
            request = self.__youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist['playlist_id'],
                maxResults = 50
            )
            resp = request.execute()
            playlist_video.extend(resp['items'])
            nextPage = resp.get('nextPageToken')
            while nextPage is not None:
                request = self.__youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist['playlist_id'],
                    maxResults=50,
                    pageToken=nextPage
                )
                resp = request.execute()
                playlist_video.extend(resp['items'])
                nextPage = resp.get('nextPageToken')
            data = self.__extract_video_id_data(playlist_video, playlist['playlist_id'])
            collection_of_video_id.extend(data)
        return collection_of_video_id
    

    def __get_video_playlist_data(self, channel_id):
        request = self.__youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50
        )
        resp = request.execute()
        data = self.__extract_playlist_data(resp['items'])
        nextPage = resp.get('nextPageToken')
        while nextPage is not None:
            request = self.__youtube.playlists().list(
                part="snippet",
                channelId=channel_id,
                maxResults=50,
                pageToken=nextPage
            )
            resp = request.execute()
            data.extend(self.__extract_playlist_data(resp['items']))
            nextPage = resp.get('nextPageToken')
        return data

    
    def get_channel_details(self, channel_id):
        data = {}
        request = self.__youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        if response['pageInfo']['totalResults'] == 0:
            st.error("Invalid Channel ID, Please check Channel ID")
        else:
            data['channel'] = self.__extract_channel_data(response)
            data['playlists'] = self.__get_video_playlist_data(data['channel']['channel_id'])
            data['videos'] = self.__get_video_data_from_playlist_with_comments(data['playlists'])
            data['comments'] = self.__get_comment_data(data['videos'])
            return data

class MongoDB:
    def __init__(self):
        self.__mongo = pymongo.MongoClient("mongodb+srv://rajans2014mech:rajan_ds_guvi@cluster0.ijeqgpt.mongodb.net/?retryWrites=true&w=majority", tlsCAFile=certifi.where())
        self.__db = self.__mongo['guvi_ds']
        self.youtube = self.__db.get_collection('youtube')
        self.video = self.__db.get_collection('video')
        self.playlist = self.__db.get_collection('playlist')
        self.comment = self.__db.get_collection('comment')
    
    def get_channel_document(self, channel_name):
        return self.get_channel_data_in_linked_structure(channel_name)
    
    def is_channel_exist_in_db(self, channel_name):
        return self.youtube.find_one({'channel_name' : channel_name})
    
    def insert_channel_document(self, data):
        channel = self.youtube.insert_one(data['channel'])
        if channel.inserted_id:
            if len(data['playlists']) > 0:
                self.playlist.insert_many(data['playlists'])
            if len(data['videos']) > 0:
                self.video.insert_many(data['videos'])
            if len(data['comments']) > 0:
                self.comment.insert_many(data['comments'])
        return channel
    
    def update_channel_document(self, data):
        remove = self.delete_channel_document(data['channel']['channel_name'])
        if remove.deleted_count > 0:
            return self.insert_channel_document(data)
        else:
            st.error(f"Failed to Update the Existing {data['channel']['channel_name']} Channel Data")
    
    def delete_channel_document(self, channel_name):
        channel = self.youtube.find_one({'channel_name' : channel_name})
        playlist_ids = [playlist['playlist_id'] for playlist in self.playlist.find({'channel_id' : channel['channel_id']})]
        for playlist_id in playlist_ids:
            video_ids = [video['video_id'] for video in self.video.find({'playlist_id' : playlist_id})]
            for video_id in video_ids:
                self.comment.delete_many({'video_id': video_id})
            self.video.delete_many({'playlist_id' : playlist_id})
        self.playlist.delete_many({'playlist_id' : {'$in' : playlist_ids}})
        return self.youtube.delete_one({'channel_name': channel_name})
    
    def get_list_of_channels_in_db(self):
        curson = self.youtube.find()
        return [doc['channel_name'] for doc in curson]
    
    def get_channel_id(self, channel_name):
        return self.youtube.find_one({'channel_name' : channel_name})['channel_id']

    def get_channel_data_in_linked_structure(self, channel_name):
        data = dict()
        data['channel'] = self.youtube.find_one({'channel_name' : channel_name})
        data['playlists'] = list()
        data['videos'] = list()
        for playlist in self.playlist.find({'channel_id': data['channel']['channel_id']}):
            data['playlists'].append(playlist)
            for video in self.video.find({'playlist_id' : playlist['playlist_id']}):
                comments = list()
                for comment in self.comment.find({'video_id' : video['video_id']}):
                    comments.append(comment)
                video['comments'] = comments
                data['videos'].append(video)
        return data

class SQL:
    def __init__(self):
        self.__connect = sqlite3.connect('sql.db')
        self.cursor = self.__connect.cursor()
        self.__create_tables_required()

    def get_sql_connection(self):
        return self.__connect
    
    def __create_tables_required(self):
        # channel table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS channel (
                channel_id VARCHAR(255) PRIMARY KEY,
                channel_name VARCHAR(255),
                subscription_count INT,
                channel_views INT,
                channel_description TEXT,
                upload_id VARCHAR(255),
                data_extracted_at VARCHAR(255)
            )
        ''')
        # playlist table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS playlist (
                playlist_id VARCHAR(255) PRIMARY KEY,
                playlist_name VARCHAR(255),
                channel_id VARCHAR(255),
                FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
            )
        ''')
        # video table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS video (
                video_id VARCHAR(255) PRIMARY KEY,
                playlist_id VARCHAR(255),
                video_name VARCHAR(255),
                video_description TEXT,
                published_at DATETIME,
                view_count INT,
                like_count INT,
                favourite_count INT,
                comment_count INT,
                duration INT,
                thumbnail VARCHAR(255),
                caption_status VARCHAR(255),
                FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id)
            )
        ''')
        # comments table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS comment (
                comment_id VARCHAR(255) PRIMARY KEY,
                comment_text TEXT,
                comment_author VARCHAR(255),
                comment_published_date_time DATETIME,
                video_id VARCHAR(255),
                FOREIGN KEY (video_id) REFERENCES video(video_id)
            )
        ''')
        self.commit_changes()

    def get_channels_in_db(self):
        self.cursor.execute("SELECT channel_name FROM channel")
        res = self.cursor.fetchall()
        res = [x[0] for x in res]
        res.sort()
        return res
    
    def get_channels_in_db_with_extracted_at(self):
        self.cursor.execute("SELECT channel_name, data_extracted_at FROM channel")
        res = self.cursor.fetchall()
        res = [[x[0], x[1]] for x in res]
        return res
    
    def get_channel_id_from_db(self, channel_name):
        self.cursor.execute("SELECT channel_id FROM channel WHERE channel_name = ? LIMIT 1",(channel_name,))
        res = self.cursor.fetchall()
        res = [x[0] for x in res]
        return res

    def commit_changes(self):
        self.__connect.commit()

    def close_connection(self):
        self.__connect.close() 

if __name__ == '__main__':
    app = AppController()
    app.controller()