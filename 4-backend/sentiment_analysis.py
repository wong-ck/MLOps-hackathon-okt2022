import pandas as pd
import statistics

from google.cloud import language_v1

GCP_BQ_URI = "dk-pop.social_media_counts.tweets"
GCP_BUCKET_SENTIMENT_CACHE_URI = "gs://dkpop_sentiment_cache/tweets/"


def download_tweets(candidate, datetime_start, duration_min):
    return


def upload_sentences_gcs(sentences, gcs_content_uri):
    return


def analyze_sentences_sentiment(gcs_content_uri):
    """
    Analyzing Sentiment in text file stored in Cloud Storage

    Args:
      gcs_content_uri Google Cloud Storage URI where the file content is located.
      e.g. gs://[Your Bucket]/[Path to File]
    """

    client = language_v1.LanguageServiceClient()

    # gcs_content_uri = 'gs://cloud-samples-data/language/sentiment-positive.txt'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"gcs_content_uri": gcs_content_uri,
                "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_sentiment(
        request={'document': document, 'encoding_type': encoding_type})
    # Get overall sentiment of the input document
    print(u"Document sentiment score: {}".format(
        response.document_sentiment.score))
    print(
        u"Document sentiment magnitude: {}".format(
            response.document_sentiment.magnitude
        )
    )
    # Get sentiment for all sentences in the document
    for sentence in response.sentences:
        print(u"Sentence text: {}".format(sentence.text.content))
        print(u"Sentence sentiment score: {}".format(sentence.sentiment.score))
        print(u"Sentence sentiment magnitude: {}".format(
            sentence.sentiment.magnitude))

    # Get the language of the text, which will be the same as
    # the language specified in the request or, if not specified,
    # the automatically-detected language.
    print(u"Language of the text: {}".format(response.language))

    # get all scores and magnitudes
    scores = [s.sentiment.score for s in response.sentences]
    magnitudes = [s.sentiment.magnitude for s in response.sentences]

    return scores, magnitudes


def upload_tweet_score(datetime, candidate, half_hour_count, positivity):
    if datetime is None:
        datetime = pd.Timestamp.now()

    df = pd.DataFrame({
        "datetime": [str(datetime)],
        "candidate": [candidate],
        "half_hour_count": [half_hour_count],
        "positivity": [positivity]})

    try:
        df.to_gbq(GCP_BQ_URI)
    except Exception as e:
        return repr(e)
    return "OK"


def main():
    candidates = ["MF", "JEJ", "MM"]
    datetime_start = pd.Timestamp.now()
    duration_min = 30

    for candidate in candidates:
        sentences = download_tweets(candidate, datetime_start, duration_min)

        gcs_content_uri = GCP_BUCKET_SENTIMENT_CACHE_URI + \
            str(datetime_start) +            \
            ".csv"
        upload_sentences_gcs(sentences, gcs_content_uri)

        scores, magnitudes = analyze_sentences_sentiment(gcs_content_uri)

        count = len(sentences)
        positivity = statistics.mean(magnitudes)
        upload_tweet_score(datetime_start,
                           candidate,
                           count,
                           positivity,)
