import os
import datetime
import isodate
import json
import sys
import logging
from googleapiclient.discovery import build
from collections import defaultdict, OrderedDict
from langdetect import detect
from math import log
import pandas as pd

MIN_DUR = isodate.parse_duration("PT1M5S")
LANGUAGE = ["EN", "FR", "ES", "RU", "HI"]
FIELDS = "items(snippet(title,publishedAt,channelTitle,channelId,thumbnails/medium/url), contentDetails(upload/videoId))"
CAPTION_MAPPING = {None: 0.975}
RATING_MAPPING = {None: 1}
DEFINITION_MAPPING = {"sd": 0.90, "hd": 1.0, "fhd": 1.025, "uhd": 1.05}
SUBSCRIBER_MAPPING = {
    10_000: 1.0375,
    100_000: 1.025,
    500_000: 1,
    1_000_000: 0.975,
    5_000_000: 0.9625,
}
DURATIONS_MAPPING = [
    (isodate.parse_duration("PT2M"), 0.500),
    (isodate.parse_duration("PT7M"), 0.950),
    (isodate.parse_duration("PT15M"), 0.975),
    (isodate.parse_duration("PT30M"), 1),
    (isodate.parse_duration("PT45M"), 1.025),
    (isodate.parse_duration("PT1H"), 1.050),
]


def search():
    global curPageToken, quotaUsage
    quotaUsage -= 100
    request = service.search().list(
        q="Programming | Tech | Computer Science",
        type="channel",
        part="id",
        maxResults=50,
        order="relevance",
        relevanceLanguage="en",
        pageToken=curPageToken,
    )

    response = request.execute()
    for item in response.get("items", []):
        try:
            tempId = item["id"]["channelId"]
            searchedChannels.append(tempId)
        except:
            pass

    curPageToken = response.get("nextPageToken")
    if curPageToken is not None:
        search()


def sortAndFilter(df):
    df.drop_duplicates(subset=["ChannelID"], inplace=True)
    df.dropna(inplace=True)
    df.sort_values(by=['SubscriberCount'], ascending=False, inplace=True)
    return df


def updateInfoChannels():
    global quotaUsage
    channels = []
    channelList = searchedChannels if searchedChannels else channelDF["ChannelID"].tolist(
    )
    for channel in channelList:
        request = service.channels().list(
            part=["snippet", "statistics", "brandingSettings"], id=channel)
        response = request.execute()
        quotaUsage -= 1

        try:
            channelInfo = {
                "ChannelID": response["items"][0]["id"],
                "ChannelName": response["items"][0]["snippet"]["title"],
                "ChannelIcon": response["items"][0]["snippet"]["thumbnails"]["medium"]["url"],
                "ChannelUrl": f'https://www.youtube.com/channel/{response["items"][0]["id"]}',
                "ExistedSince": response["items"][0]["snippet"]["publishedAt"].split("T")[0],
                "SubscriberCount": int(response["items"][0]["statistics"].get("subscriberCount", 0)),
                "VideoCount": int(response["items"][0]["statistics"].get("videoCount", 0)),
                "ViewCount": int(response["items"][0]["statistics"].get("viewCount", 0)),
                "Country": response["items"][0]["snippet"].get("country", "Unknown"),
                "Language": response["items"][0]["snippet"].get("defaultLanguage", "Unknown")
            }

            if channelInfo["SubscriberCount"] > 10_000:
                channels.append(channelInfo)
        except:
            pass

    df = sortAndFilter(pd.DataFrame(channels))
    df.to_csv("channels.csv", index=False)


def allMightyAlgorithm(video: json, vidDuration: isodate, SubscriberCount: int) -> int:
    NRLLikeCount = log(video["LikeCount"] + 1)
    NRLCommentCount = log(video["CommentCount"] + 1)
    NRLViewCount = log(video["ViewCount"] + 1)

    viewRate = NRLViewCount / 3.25
    # prevent ZeroDivisionError
    likeRate = NRLLikeCount / NRLViewCount if NRLViewCount > 0 else 0
    commentRate = (NRLCommentCount / NRLViewCount) * \
        1.25 if NRLViewCount > 0 else 0  # prevent ZeroDivisionError

    defQuality = DEFINITION_MAPPING.get(video["Definition"].lower(), 1)
    capQuality = CAPTION_MAPPING.get(video["Caption"], 1)
    ratQuality = RATING_MAPPING.get(video["ContentRating"], 0.925)
    durQuality = next(
        (quality for duration, quality in DURATIONS_MAPPING if vidDuration < duration), 0.90)
    subBalance = next((balance for channel, balance in SUBSCRIBER_MAPPING.items(
    ) if SubscriberCount < channel), 0.925)

    qualityMultiplier = float(
        subBalance * defQuality * capQuality * ratQuality * durQuality)
    rating = round((viewRate + likeRate + commentRate)
                   * qualityMultiplier * 100, 3)
    return rating


def fetchNewVideos():
    global quotaUsage
    videosIds = []
    for channel in channelDF["ChannelID"]:
        subscriberCount = channelDF[channelDF["ChannelID"]
                                    == channel]["SubscriberCount"].values[0]
        request = service.activities().list(
            part=["snippet", "id", "contentDetails"],
            channelId=channel,
            publishedAfter=today.isoformat() + "T00:00:00Z",
            maxResults=5,
            fields=FIELDS,
        )

        response = request.execute()
        quotaUsage -= 1
        for item in response["items"]:
            try:
                channelName = item["snippet"]["channelTitle"]
                channelId = item["snippet"]["channelId"]
                videoTitle = item["snippet"]["title"]
                videoId = item["contentDetails"]["upload"]["videoId"]
                publishedAt = item["snippet"]["publishedAt"][:16].replace(
                    "T", " ")
                thumbnailUrl = item["snippet"]["thumbnails"]["medium"]["url"]

                request = service.videos().list(
                    id=videoId, part=["statistics", "snippet", "contentDetails"])
                response = request.execute()
                quotaUsage -= 1

                viewCount = int(response["items"][0]
                                ["statistics"].get("viewCount", 0))
                likeCount = int(response["items"][0]
                                ["statistics"].get("likeCount", 0))
                commentCount = int(
                    response["items"][0]["statistics"].get("commentCount", 0))
                categoryId = int(response["items"][0]["snippet"]["categoryId"])
                contentRating = response["items"][0]["contentDetails"].get(
                    "contentRating")
                definition = response["items"][0]["contentDetails"]["definition"]
                duration = isodate.parse_duration(
                    response["items"][0]["contentDetails"]["duration"])
                caption = response["items"][0]["contentDetails"].get(
                    "caption", "false")
                language = str(
                    response["items"][0]["snippet"].get("defaultLanguage",
                                                        response["items"][0]["snippet"].get("defaultAudioLanguage", "NONE"))
                ).upper()
                language = "HI" if "HI" in language else language[:2]

                try:
                    channel_language = channelDF[channelDF["ChannelID"]
                                                 == channelId]["Language"].values[0]
                except IndexError:
                    channel_language = "Unknown"

                language = "HI" if channel_language == "HI" else language
                language = detect(videoTitle).upper() if language in [
                    'NONE', 'ZXX'] else language

                if videoId not in videosIds and viewCount > 750 and MIN_DUR < duration and language in LANGUAGE:
                    fullVideoDetails = {
                        "ChannelName": channelName,
                        "ChannelId": channelId,
                        "ChannelIcon": channelDF[channelDF["ChannelID"] == channelId]["ChannelIcon"].values[0],
                        "ChannelUrl": channelDF[channelDF["ChannelID"] == channelId]["ChannelUrl"].values[0],
                        "VideoUrl": f"https://www.youtube.com/watch?v={videoId}",
                        "VideoTitle": videoTitle,
                        "VideoId": videoId,
                        "PublishedDate": publishedAt,
                        "Thumbnail": thumbnailUrl,
                        "Duration": str(duration).split(", ")[1] if ", " in str(duration) else str(duration),
                        "Definition": str(definition).upper(),
                        "language": language,
                        "Caption": False if caption == "false" else True,
                        "ContentRating": False if not contentRating else True,
                        "ViewCount": int(viewCount),
                        "LikeCount": int(likeCount),
                        "CommentCount": int(commentCount),
                        "CategoryId": int(categoryId)
                    }

                    videoRating = allMightyAlgorithm(
                        fullVideoDetails, duration, subscriberCount)
                    Videos[language][videoRating] = fullVideoDetails
                    videosIds.append(videoId)
            except Exception as e:
                logging.error(f"Error in fetchNewVideos inner loop: {e}")
                pass


def getNewData(video):
    global quotaUsage
    try:
        request = service.videos().list(id=video["VideoId"], part=[
            "statistics", "snippet", "contentDetails"])
        response = request.execute()
        quotaUsage -= 1
        fullVideoDetails = {
            "ChannelName": video["ChannelName"],
            "ChannelId": video["ChannelId"],
            "ChannelIcon": video["ChannelIcon"],
            "ChannelUrl": video["ChannelUrl"],
            "VideoUrl": video["VideoUrl"],
            "VideoTitle": response["items"][0]["snippet"]["title"],
            "VideoId": video["VideoId"],
            "PublishedDate": video["PublishedDate"],
            "Thumbnail": response["items"][0]["snippet"]["thumbnails"]["medium"]["url"],
            "Duration": video["Duration"],
            "Definition": video["Definition"],
            "language": video["language"],
            "Caption": False if response["items"][0]["contentDetails"].get("caption", "false") == "false" else True,
            "ContentRating": False if not response["items"][0]["contentDetails"].get("contentRating") else True,
            "ViewCount": int(response["items"][0]["statistics"].get("viewCount", 0)),
            "LikeCount": int(response["items"][0]["statistics"].get("likeCount", 0)),
            "CommentCount": int(response["items"][0]["statistics"].get("commentCount", 0)),
            "CategoryId": int(video["CategoryId"])
        }

        return fullVideoDetails, isodate.parse_duration(response["items"][0]["contentDetails"]["duration"])
    except Exception as e:
        logging.error(f"Error in getNewData: {e}")
        pass


def checkOldVideos(time, date):
    threshold = {"weekly": 7, "monthly": 30, "yearly": 365}
    timeline = datetime.timedelta(days=threshold[time])
    date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M")
    return (datetime.datetime.now() - date) < timeline


def updateVideos(allVideos, time):
    result = {}
    for videoRating, video in allVideos.items():
        if checkOldVideos(time, video["PublishedDate"]):
            newData = getNewData(video)
            if newData:  # Check if getNewData returned something
                video, duration = newData
                subcriberCount = channelDF[channelDF["ChannelID"] ==
                                           video['ChannelId']]["SubscriberCount"].values[0]
                videoRating = allMightyAlgorithm(
                    video, duration, subcriberCount)
                result[videoRating] = video

    return result


def sortAccordingly(allVideos):
    existingChannels = []
    filteredVideos = OrderedDict()
    allVideos = OrderedDict(
        sorted(allVideos.items(), key=lambda item: float(item[0]), reverse=True))
    for videoRating, video in allVideos.items():
        if video["ChannelId"] not in existingChannels:
            existingChannels.append(video["ChannelId"])
            filteredVideos[videoRating] = video
    return filteredVideos


def storeVideos():
    topDay, topWeek, topMonth = {}, {}, {}
    for lang, videos in Videos.items():
        for time in ["daily", "weekly", "monthly", "yearly"]:
            with open(f"{time}.json", "r") as f:
                data = json.load(f)

            if time == "daily":
                topDay = OrderedDict(
                    sorted(videos.items(), key=lambda item: float(item[0]), reverse=True))
                data[lang] = dict(list(topDay.items())[:40])

            elif time == "weekly":
                topWeek = updateVideos(data.get(lang, {}), time)
                topWeek.update(dict(list(topDay.items())[:5]))
                topWeek = sortAccordingly(topWeek)
                data[lang] = dict(topWeek)

            elif time == "monthly":
                if today.weekday() == 0:
                    topMonth = updateVideos(data.get(lang, {}), time)
                    topMonth.update(dict(list(topWeek.items())[:10]))
                    topMonth = sortAccordingly(topMonth)
                    data[lang] = dict(topMonth)

            elif time == "yearly":
                if today.day == 1:
                    if today.weekday() != 0:
                        with open("monthly.json", "r") as f:
                            newData = json.load(f)
                            topMonth = newData.get(lang, {})

                    topYear = updateVideos(data.get(lang, {}), time)
                    topYear.update(dict(list(topMonth.items())[:5]))
                    data[lang] = dict(sortAccordingly(topYear))

            with open(f"{time}.json", "w") as f:
                json.dump(data, f, indent=4)


def main():
    global channelDF, service, today, Videos, quotaUsage, searchedChannels, curPageToken
    logging.basicConfig(filename='youtube.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    API_KEY = os.environ.get("YT_API_KEY")
    service = build("youtube", "v3",
                    developerKey=API_KEY)
    today = datetime.date.today() - datetime.timedelta(days=1)
    Videos = defaultdict(dict)
    quotaUsage = 10_000
    searchedChannels = []
    curPageToken = None

    # Check if the channels.csv file exists
    if not os.path.exists("channels.csv"):
        logging.info("channels.csv not found. Performing search and update.")
        search()
        updateInfoChannels()
        channelDF = pd.read_csv("channels.csv")
    else:
        channelDF = pd.read_csv("channels.csv")

    if len(sys.argv) > 1 and sys.argv[1] == "search":
        search()
        updateInfoChannels()
        exit(0)

    try:
        updateInfoChannels() if (today.day % 8 == 0 and today.weekday() != 0) else None
        fetchNewVideos()
        storeVideos()

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    else:
        logging.info(f"Remaining quota for this day : {quotaUsage}")


if __name__ == "__main__":
    main()
