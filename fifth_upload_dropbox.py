import yt_dlp
import re


def get_yt_playlist(playlist_url):
    ydl_opts = {
        'ignoreerrors': True,
        'quiet': True,
        'extract_flat': 'in_playlist',
        'dump_single_json': True,
        'playlistend': 100000,  # 設定一個足夠大的數字以確保下載整個清單
        #         'cookiefile': '/kaggle/working/cookies_yt.txt'  # Specify the path to your cookie file
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        playlist_info = ydl.extract_info(playlist_url, download=False)

    videos = list()
    fail_url_list = list()
    for entry in playlist_info['entries']:
        if entry == None:
            print('entry is None')
            continue
        if entry['channel'] == None:
            #             print(f'unknown problem entry {entry}')
            fail_url_list.append(entry['url'])
            continue

        video = {
            'title': re.sub(r'(#[^\s]+|\||\/)', '', entry['title']),
            'channel': entry['channel'][:15] if len(entry['channel']) > 15 else entry['channel'],
            # (text[:15] + "...") if len(text) > 15 else text
            'url': entry['url'],
            'duration': entry['duration']
        }

        if entry.get('live_status', None) != 'is_upcoming':
            videos.append(video)
            # debug
            # print(f"entry handle {video['title']}")
        else:
            print(f'is upcoming {video}')

    #     if len(fail_url_list):
    #         remove_videos_from_playlist(playlist_url, fail_url_list)

    return videos


from pytube import YouTube

def get_upload_time(url):
    try:
        yt = YouTube(url)
        upload_time = yt.publish_date
        if upload_time:
            return upload_time.strftime('%Y-%m-%d')  # 格式化为 YYYY-MM-DD HH:MM:SS
        else:
            return "Upload time not available"
    except Exception as e:
        print(f"Error retrieving upload time for {url}: {e}")
        return None


from pymongo import MongoClient

# 连接到 MongoDB
client = MongoClient("mongodb+srv://chendoit:dddd2222@cluster0.qiset7z.mongodb.net/?retryWrites=true&w=majority")
col = client.youtube_download.youtube_data


def insert_dict_to_mongodb(col, data_dict):
    try:
        # 使用 "url" 字段作为查询条件，尝试在集合中插入字典
        col.insert_one(data_dict)
        # 如果插入成功，则返回 True
        return True
    except Exception as e:
        # 如果发生异常，则返回 False
        # print("Failed to insert data into MongoDB:", str(e))
        return False


def update_document_in_mongodb(col, query, update_data):
    try:
        # 使用传入的查询条件更新文档
        col.update_one(query, {"$set": update_data})
        # 如果更新成功，则返回 True
        return True
    except Exception as e:
        # 如果发生异常，则返回 False
        # print("Failed to update document in MongoDB:", str(e))
        return False


def query_documents_in_mongodb(col, query):
    try:
        # 使用传入的查询条件在集合中查询文档
        result = col.find(query)
        # 将查询结果转换为列表并返回
        return list(result)
    except Exception as e:
        # 如果发生异常，则返回空列表
        # print("Failed to query documents in MongoDB:", str(e))
        return []


from datetime import datetime, timedelta


def check_download_eligibility(col, video):
    #     return True

    url = video.get("url")
    query = {"url": url}
    document = col.find_one(query)

    if document:
        state = document.get("state")
        timestamp = document.get("timestamp")

        if state == "done":
            #             print(f"video {video['title']} done")
            return False

        if state == "init":
            current_time = datetime.now()
            if timestamp + timedelta(hours=8) <= current_time:
                print(f"video {video['title']} overtime start again")
                video['timestamp'] = datetime.now()
                update_document_in_mongodb(col, {'url': video['url']}, video)
                return True
            else:
                print(f"video {video['title']} less time, downloading")
                return False

    else:
        video["state"] = "init"
        video["timestamp"] = datetime.now()
        try:
            col.insert_one(video)
            print(f"video {video['title']} start download")
        except:
            print(f"video {video['title']} insert failure")
            return False

    return True



app_key = 'mdfnkmu9msgft5p'
app_secret = 'btux7hz69iexdpp'
access_token = "Bk4w_3Ng6zy_Ut-jgdmupkXQVZM_AiCA0aGfdp8pgyLwUe5eujmGG2OmvnKUGxHvKqyfO7d-TaTAkWiSS9RyoNgbNscJDY2nI8QxYmemgd_6dAnCcrdIAgttq6qdpoVCtEkm-u0ophbpF8hqg7YYr3Q"
refresh_token = 'gX0BP9bRhiYAAAAAAAAAAYgr3whI5VMX3TzROYE4EvoBkZdovUiFcuBCwyMIB1Jl'

import dropbox
import json
import os


def upload_to_dropbox(access_token, local_file_path, video_metadata, upload_to_dropbox=False):
    if upload_to_dropbox:
        try:
            dbx = dropbox.Dropbox(app_key=app_key, app_secret=app_secret, oauth2_refresh_token=refresh_token)

            # Define the Dropbox directory path
            dropbox_directory = '/youtube_暫存/'

            # Upload the MP3 file
            dropbox_mp3_path = dropbox_directory + local_file_path
            with open(local_file_path, 'rb') as f:
                dbx.files_upload(f.read(), dropbox_mp3_path, mode=dropbox.files.WriteMode('overwrite'))

            os.remove(local_file_path)

            # Create and upload the JSON file
            json_file_path = local_file_path.replace('.mp3', '.json')
            dropbox_json_path = dropbox_directory + os.path.basename(json_file_path)
            with open(json_file_path, 'w') as f:
                json.dump(video_metadata, f, indent=4)

            with open(json_file_path, 'rb') as f:
                dbx.files_upload(f.read(), dropbox_json_path, mode=dropbox.files.WriteMode('overwrite'))

            os.remove(json_file_path)
            print(f'Files uploaded to Dropbox: {dropbox_mp3_path}, {dropbox_json_path}')
        except Exception as e:
            print(f'Error uploading to Dropbox: {e}')
    else:

        json_file_path = local_file_path.replace('.mp3', '.json')
        with open(json_file_path, 'w') as f:
            json.dump(video_metadata, f, indent=4)



import dropbox
import re


def list_files_from_dropbox(access_token):
    try:
        # Initialize Dropbox client
        dbx = dropbox.Dropbox(app_key=app_key, app_secret=app_secret, oauth2_refresh_token=refresh_token)

        # Define Dropbox directory path
        dropbox_directory = '/youtube_暫存/'

        # List files in the directory
        result = dbx.files_list_folder(dropbox_directory)
        file_names = [entry.name for entry in result.entries if isinstance(entry, dropbox.files.FileMetadata)]

        # Define the regex pattern for filenames
        pattern = re.compile(r'^\d{4}_\d{2}_\d{2}_\d{10}\.(json|mp3)$')

        # Filter files based on the pattern
        matching_files = [name.split('.')[0] for name in file_names if pattern.match(name)]

        return list(set(matching_files))

    except Exception as e:
        print(f'Error listing files from Dropbox: {e}')
        return []


# 连接到 MongoDB
client = MongoClient("mongodb+srv://chendoit:dddd2222@cluster0.qiset7z.mongodb.net/?retryWrites=true&w=majority")
col = client.youtube_download.youtube_data

import random
import os
import time
from datetime import datetime, timedelta
import pytz
import yt_dlp


def generate_new_filename():
    today = datetime.now().strftime('%Y_%m_%d')
    random_number = random.randint(1000000000, 9999999999)
    return f'{today}_{random_number}.mp3'


def download_yt_video(url, filename):
    base_filename, ext = os.path.splitext(filename)
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': base_filename,
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


def process_videos(not_downloaded_videos, col, access_token):
    for video in not_downloaded_videos:
        if check_download_eligibility(col, video):
            start_time = time.time()
            now = datetime.now(pytz.timezone(r"Asia/Taipei"))
            print(f"Video download start {now} {video['title']}")

            md_file = '_'.join([video['channel'], video['title']])
            md_file = md_file[:60] + '.md'

            upload_time = get_upload_time(video['url'])
            mp3_file_name = generate_new_filename()
            download_yt_video(video['url'], mp3_file_name)

            video_metadata = {
                'channel': video['channel'],
                'title': video['title'],
                'url': video['url'],
                'duration': video['duration'],
                'upload_time': upload_time,
                'md_file': md_file
            }

            upload_to_dropbox(access_token, mp3_file_name, video_metadata)

            print(f"Video {video['title']} {upload_time}")

            video['timestamp'] = datetime.now()
            video['state'] = 'done'
            update_document_in_mongodb(col, {'url': video['url']}, video)
            time.sleep(10)

        else:
            if video in not_downloaded_videos[-10:]:
                pass
                # print(f"downloaded {video['title']}")

if __name__ == '__main__':

    # playlist_url = 'https://www.youtube.com/playlist?list=PL0P8pZm-KxhRbmxH4H6-Zr-DDfrwiMvJg' #直播
    playlist_url = 'https://www.youtube.com/playlist?list=PL0P8pZm-KxhQao6np_egWEBPjJsELEpQX'  # transcript
    # playlist_url = 'https://www.youtube.com/playlist?list=PL0P8pZm-KxhTjx6s-o-kESRJyb9TASAtT'

    playlist = get_yt_playlist(playlist_url)
    downloaded_list = query_documents_in_mongodb(col, {'state': 'done'})
    downloaded_list = [_['url'] for _ in downloaded_list]

    all_download = True
    for _ in playlist:
        if _['url'] not in downloaded_list:
            print('not downloaded', _)
            all_download = False
    if all_download:
        print('all video in playlist are downloaded')

    not_downloaded_videos = [video for video in playlist if video['url'] not in downloaded_list]
    process_videos(not_downloaded_videos, col, access_token)