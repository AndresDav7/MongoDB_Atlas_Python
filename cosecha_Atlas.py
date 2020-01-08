import tweepy
import json
from pymongo import MongoClient

Palabras = ['#WWIII', 'EEUU', 'Iran', 'Irak']

CONSUMER_KEY = "BZ1ivxg5dW1QSQEkHMbXurIpu"
CONSUMER_SECRET = "rjlMbsTIMFMCFuIgwQ5NaAOrfntHvtpI94tsWIfFnoPjQT9kAk"
ACCESS_TOKEN = "976534223266942977-iXyEjZyf7nnabNqNhtdkBK4Ny1EG5JN"
ACCESS_TOKEN_SECRET = "OZTAm5Q2tV6tP8LWdLUyz6xY7vDLMeL9OSDLOWcsp3L7o"

class StreamListener(tweepy.StreamListener):

    def on_connect(self):
        print("Conectado a la API de transmision")

    def on_error(self, status_code):
        print('Error --> ' + repr(status_code))
        return False

    def on_data(self, data):
        try:
            client = MongoClient("mongodb://andres:andres123@deber-pythonatlas-shard-00-00-clz58.mongodb.net:27017/test?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=true")
            db = client.base_twitter
            datajson = json.loads(data)
            created_at = datajson['created_at']
            print("Tweet almacenado el --> " + str(created_at))
            db.coleccion_twitter.insert(datajson)
        except Exception as e:
           print(e)

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Buscando: " + str(Palabras))
streamer.filter(track=Palabras)