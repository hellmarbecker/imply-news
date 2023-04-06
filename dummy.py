import time
import argparse, sys, logging
import socket
import signal
import os

reconfigure = False

# Signal handlers

def hupHandler(signum, frame):
    global reconfigure
    reconfigure = True
    logging.debug("in signal handler: SIGHUP received!")


# --- Main entry point ---

def main():

    global reconfigure

    # Install signal handlers

    # SIGHUP triggers a config reread
    signal.signal(signal.SIGHUP, hupHandler)

    # SIGUSR1 is used as a live check signal - ignore and just use the return code of kill(1) in the watchdog
    signal.signal(signal.SIGUSR1, signal.SIG_IGN)

    logLevel = logging.DEBUG
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logLevel)

    pid = os.getpid()
    logging.debug(f'my pid: {pid}')

    while True:

        logging.debug(f'{pid} Top of outer loop')
        reconfigure = False

        while not reconfigure:

            logging.debug(f'{pid} Top of inner loop')
            waitSecs = 30
            logging.debug(f'wait time: {waitSecs}')
            time.sleep(waitSecs)
        

if __name__ == "__main__":
    main()
