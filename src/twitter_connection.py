from src.tweet_listener import TweetListener
from src import credentials
import socket
import sys


def create_socket(host='0.0.0.0', port=5555):
    sock = socket.socket()
    sock.bind((host, port))
    print('socket has been setup')
    return sock


if __name__ == '__main__':
    if len(sys.argv) == 1:
        raise ValueError('Not enough arguments have been passed!')
    sock = create_socket()
    sock.listen(4)
    print('socket is listening...')
    print(sys.argv[1:])
    client_socket, address = sock.accept()
    print('received request from: ' + str(address))
    listener = TweetListener(credentials.BEARER_TOKEN, client_socket)
    listener.send_data(keywords=sys.argv[1:])