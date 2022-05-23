import json
import logging
import tweepy
from tweepy.streaming import StreamingClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class TweetListener(StreamingClient):

    def __init__(self, bearer_token, client_socket):
        super().__init__(bearer_token, wait_on_rate_limit=True)
        self._client_socket = client_socket

    def on_data(self, data):
        try:
            message = json.loads(data)
            print(message['data']['text'])
            self._client_socket.send(
                str(message['data']['text'] + 'tweet_end').encode('utf-8'))
            return True
        except BaseException as ex:
            logger.error('Data error: ' + str(ex))
        return True

    def on_connection_error(self):
        logger.error('Error while connecting')
        self.disconnect()

    def on_request_error(self, status_code):
        logger.error('Non-200 HTTP status encountered: ' + str(status_code))

    # Helper function to get keywords passed as arguments and make rules for tweet filtering
    def _make_rules(self, keywords):
        keyword_rule = ['(']
        lang = 'en'
        for index, kw in enumerate(keywords):
            if kw == '-lang':
                lang = keywords[index + 1]
                break
            keyword_rule.append(str(kw))

        keyword_rule = ' OR '.join(keyword_rule) + ')'
        keyword_rule = keyword_rule.replace(' OR ', '', 1)
        keyword_rule += (' lang:' + lang)
        return keyword_rule

    def send_data(self, keywords):
        logger.info('Start sending data from Twitter to socket.')
        print(keywords)
        stream_rule = self._make_rules(keywords)
        print(stream_rule)
        self.add_rules(tweepy.StreamRule(stream_rule))
        self.filter()