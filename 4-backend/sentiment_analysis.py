import os
import statistics

import pandas as pd
from logic import get_reddits, reddits_to_df, get_tweets, search_tweets, tweets_to_df

from google.oauth2 import service_account
from google.cloud import language_v1
from google.cloud import storage

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

GCP_BQ_URI = "social_media_counts.tweets"
# GCP_BUCKET_SENTIMENT_CACHE_URI = "gs://dkpop_sentiment_cache/tweets/"
GCP_BUCKET_SENTIMENT_CACHE_NAME = "dkpop_sentiment_cache"
GCP_SA_CRED_JSON = os.path.join(ROOT_DIR, "dk-pop-9234b58dce83.json")


def download_tweets(candidate, datetime_start, duration_min):
    if candidate == "MF":
        q = "#mettefrederiksen OR @Statsmin"
    elif candidate == "JEJ":
        q = "@JakobEllemann"
    elif candidate == "MM":
        q = "@MrMesserschmidt"
    else:
        raise Exception("whut?")

    tweets = search_tweets(q, limit=100)
    tweets_df = tweets_to_df(tweets)
    return tweets_df


def upload_sentences_gcs(sentences, bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client.from_service_account_json(GCP_SA_CRED_JSON)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    with blob.open("w", encoding="utf-8") as f:
        for sentence in sentences:
            f.write(sentence + "\n")

    # with blob.open("r", encoding="utf-8") as f:
    #     print(f.read())

    return


def analyze_sentences_sentiment(gcs_content_uri):
    """
    Analyzing Sentiment in text file stored in Cloud Storage

    Args:
      gcs_content_uri Google Cloud Storage URI where the file content is located.
      e.g. gs://[Your Bucket]/[Path to File]
    """
    client = language_v1.LanguageServiceClient.from_service_account_json(
        GCP_SA_CRED_JSON)

    # gcs_content_uri = 'gs://cloud-samples-data/language/sentiment-positive.txt'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "de"
    document = {"gcs_content_uri": gcs_content_uri,
                "type_": type_, "language": language,
                }

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_sentiment(
        request={'document': document, 'encoding_type': encoding_type})
    # # Get overall sentiment of the input document
    # print(u"Document sentiment score: {}".format(
    #     response.document_sentiment.score))
    # print(
    #     u"Document sentiment magnitude: {}".format(
    #         response.document_sentiment.magnitude
    #     )
    # )
    # # Get sentiment for all sentences in the document
    # for sentence in response.sentences:
    #     print(u"Sentence text: {}".format(sentence.text.content))
    #     print(u"Sentence sentiment score: {}".format(sentence.sentiment.score))
    #     print(u"Sentence sentiment magnitude: {}".format(
    #         sentence.sentiment.magnitude))

    # # Get the language of the text, which will be the same as
    # # the language specified in the request or, if not specified,
    # # the automatically-detected language.
    # print(u"Language of the text: {}".format(response.language))

    # get all scores and magnitudes
    scores = [s.sentiment.score for s in response.sentences]

    return scores


def remove_sentences_gcs(bucket_name, blob_name):
    storage_client = storage.Client.from_service_account_json(GCP_SA_CRED_JSON)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()


def upload_tweet_score(datetime, candidate, half_hour_count, positivity):
    if datetime is None:
        datetime = pd.Timestamp.now()

    credentials = service_account.Credentials.from_service_account_file(
        GCP_SA_CRED_JSON)

    df = pd.DataFrame({
        "datetime": [datetime],
        "candidate": [candidate],
        "half_hour_count": [half_hour_count],
        "positivity": [positivity]})

    try:
        df.to_gbq(GCP_BQ_URI, credentials=credentials, if_exists="append")
    except Exception as e:
        print(e)
        return repr(e)
    return "OK"


def main():
    candidates = ["MF", "JEJ", "MM"]
    datetime_start = pd.Timestamp.now()
    duration_min = 30
    for candidate in candidates:
        tweets_df = download_tweets(candidate, datetime_start, duration_min)

        sentences = tweets_df["text"].tolist()
        sentences = [str(s).replace("\n", '') for s in sentences]

        # gcs_content_uri = GCP_BUCKET_SENTIMENT_CACHE_URI + \
        #     str(datetime_start) +            \
        #     ".csv"
        # upload_sentences_gcs(sentences, gcs_content_uri)

        bucket_name = GCP_BUCKET_SENTIMENT_CACHE_NAME
        blob_name = f"tweet_{str(datetime_start)}_{candidate}.csv"
        upload_sentences_gcs(
            sentences, GCP_BUCKET_SENTIMENT_CACHE_NAME, blob_name)

        gcs_content_uri = f"gs://{bucket_name}/{blob_name}"
        scores = analyze_sentences_sentiment(gcs_content_uri)

        # remove_sentences_gcs(GCP_BUCKET_SENTIMENT_CACHE_NAME, blob_name)

        count = len(sentences)
        positivity = statistics.mean(scores)
        upload_tweet_score(datetime_start,
                           candidate,
                           count,
                           positivity,)
