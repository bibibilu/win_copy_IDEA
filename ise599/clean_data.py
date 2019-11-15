# Emoji patterns
import os
import pandas as pd
import tweepy
import re
import string
from textblob import TextBlob
from .api import clean
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk
nltk.download('stopwords')
nltk.download('punkt')

COLS = ['id', 'created_at', 'source', 'original_text', 'clean_text', 'sentiment', 'polarity', 'subjectivity', 'lang',
        'favorite_count', 'retweet_count', 'original_author', 'possibly_sensitive', 'hashtags',
        'user_mentions', 'place', 'place_coord_boundaries']

emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)

# Happy Emoticons
emoticons_happy = set([
    ':-)', ':)', ';)', ':o)', ':]', ':3', ':c)', ':>', '=]', '8)', '=)', ':}',
    ':^)', ':-D', ':D', '8-D', '8D', 'x-D', 'xD', 'X-D', 'XD', '=-D', '=D',
    '=-3', '=3', ':-))', ":'-)", ":')", ':*', ':^*', '>:P', ':-P', ':P', 'X-P',
    'x-p', 'xp', 'XP', ':-p', ':p', '=p', ':-b', ':b', '>:)', '>;)', '>:-)',
    '<3'
])

# Sad Emoticons
emoticons_sad = set([
    ':L', ':-/', '>:/', ':S', '>:[', ':@', ':-(', ':[', ':-||', '=L', ':<',
    ':-[', ':-<', '=\\', '=/', '>:(', ':(', '>.<', ":'-(", ":'(", ':\\', ':-c',
    ':c', ':{', '>:\\', ';('
])

emoticons = emoticons_happy.union(emoticons_sad)


# mrhod clean_tweets()
def clean_tweets(tweet):
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(tweet)

    # remove emojis from tweet
    tweet = emoji_pattern.sub(r'', tweet)

    filtered_tweet = [w for w in word_tokens if not w in stop_words]

    # looping through conditions
    for w in word_tokens:
        # check tokens against stop words , emoticons and punctuations
        if w not in stop_words and w not in emoticons and w not in string.punctuation:
            filtered_tweet.append(w)
    return ' '.join(filtered_tweet)
    # print(word_tokens)
    # print(filtered_sentence)
    # filter using NLTK library append it to a string
    filtered_tweet = [w for w in word_tokens if not w in stop_words]
    filtered_tweet = []

    # looping through conditions
    for w in word_tokens:
        # check tokens against stop words , emoticons and punctuations
        if w not in stop_words and w not in emoticons and w not in string.punctuation:
            filtered_tweet.append(w)
    return ' '.join(filtered_tweet)


df = pd.read_json('C:/Users/Claire/IdeaProjects/ise599/tweets (1).json', lines=True)

new_entry = []

for i in range(df.shape[0]):

    # tweepy preprocessing called for basic preprocessing
    clean_text = clean(df['text'][i])
    # call clean_tweet method for extra preprocessing
    filtered_tweet = clean_tweets(df["text"][i])

    # remove emojis from tweet

    filtered_tweet = clean_tweets.lower()  # convert lowercase
    filtered_tweet = filtered_tweet.translate(str.maketrans('', '', string.punctuation))  # remove punctuation

    if len(filtered_tweet.strip().split()) > 5:  # must have 5 more words
        blob = TextBlob(filtered_tweet)
        Sentiment = blob.sentiment

        # seperate polarity and subjectivity in to two variables
        polarity = Sentiment.polarity
        subjectivity = Sentiment.subjectivity
        print(filtered_tweet)
        #new_entry.append([df['created_at'][i], filtered_tweet, Sentiment, polarity, subjectivity, df['label'][i]])

newdataset = pd.DataFrame(new_entry,
                          columns=['created_at', "filtered_tweet", "Sentiment", "polarity", "subjectivity", 'label'])
print(newdataset.shape)
newdataset.to_csv(r'C:/Users/Claire/IdeaProjects/ise599/cleanedData1023.csv')