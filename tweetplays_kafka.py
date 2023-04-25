from kafka import KafkaProducer, KafkaConsumer
import json

# Define Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define Kafka consumer configuration
consumer = KafkaConsumer(
    'nfl_plays',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Update tweet function
def update_tweet(tweet):
    # Update Twitter status using Tweepy API
    auth = tweepy.OAuthHandler(
        twitter_auth_keys['consumer_key'],
        twitter_auth_keys['consumer_secret']
    )
    auth.set_access_token(
        twitter_auth_keys['access_token'],
        twitter_auth_keys['access_token_secret']
    )
    api = tweepy.API(auth)
    try:
        status = api.update_status(status=tweet)
        print(f'Successfully tweeted: {tweet}')
    except tweepy.errors.Forbidden as e:
        print(f'Error: {e}')

# Scrape NFL plays and send them to Kafka topic
def scrape_nfl_plays():
    website = pd.read_html('https://nytimes.stats.com/fb/pbp.asp?gamecode=20230108023&home=23&vis=5|')
    plays = pd.DataFrame()
    df = website[5]
    plays = plays.append(df)
    plays.drop_duplicates()
    plays.to_csv('plays.csv')
    to_kafka = pd.read_csv('plays.csv')
    for i in to_kafka['0'].head(2):          
        string = i
        print(string)
        if 'TOUCHDOWN' in string:
            tweet = i + '\n' + hashtag1 +  '\n' + hashtag2
            producer.send('nfl_plays', {'tweet': tweet, 'type': 'touchdown'})
        elif 'Penalty' in string:
            tweet = i + '\n' + hashtag1 +  '\n' + hashtag2
            producer.send('nfl_plays', {'tweet': tweet, 'type': 'penalty'})
        # add more conditions for other types of plays

# Read data from Kafka topic and update tweets accordingly
def process_nfl_plays():
    for message in consumer:
        play_type = message.value['type']
        tweet = message.value['tweet']
        if play_type == 'touchdown' or play_type == 'Penalty':
            update_tweet(tweet)

# Schedule tasks to run periodically using Threading Timer
def schedule_tasks():
    threading.Timer(5, scrape_nfl_plays).start()
    threading.Timer(10, process_nfl_plays).start()

# Run the program
if __name__ == '__main__':
    schedule_tasks()
