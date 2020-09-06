import json
import random
import time

baseurl = "https://imply-shop.com"

class Session:

    def __init__(self):
        self.state = "initial"
        self.utm_campaign = "Google"

    def advance()
        if self.state == "initial":
            self.state = "next"

def main():

    while True:
        # With a certain probability, create a new session
        if random.random() < 0.05:
            newSession = Session()
            allSessions.append(newSession)
        # Pick one of the sessions
        thisSession = random.choice(allSessions)
        thisSession.advance()
        

if __name__ == "__main__":
    main()
