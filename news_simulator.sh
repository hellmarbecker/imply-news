#!/bin/bash
#
# Based on code by ahadjidj

# news_secret.sh must define the variables CC_BOOTSTRAP, CC_APIKEY and CC_SECRET
. ./news_secret.sh

BASE=~/imply-shop
PID=/tmp/news_simulator.pid
NORMAL=/tmp/normal.flag
ABNORMAL=/tmp/abnormal.flag
LOG=/tmp/news_simulator.log
ERROR=/tmp/news_simulator-error.log
CONFIG=news_config.yml
CMD=news_process.py
KAFKACAT_CC="kafkacat -t imply-news -b ${CC_BOOTSTRAP} -K \"|\" \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanism=PLAIN \
    -X sasl.username=${CC_APIKEY} \
    -X sasl.password=${CC_SECRET}"
COMMAND_NORMAL="python3 $BASE/$CMD -f $BASE/$CONFIG -m default | "
COMMAND_ABNORMAL="python3 $BASE/$CMD -f $BASE/$CONFIG -m after_fix"

echo $KAFKACAT_CC
exit 0

status() {
    echo
    echo "==== Status"

    if [ -f $PID ]
    then
        echo
        echo "Pid file: $( cat $PID ) [$PID]"
        echo
        ps -ef | grep -v grep | grep $( cat $PID )
        if [ -f $NORMAL ] 
        then
            echo
            echo "Running in normal mode"
            echo
        else
            if [ -f $ABNORMAL ]
            then
                echo
                echo "Running in abnormal mode"
                echo
            else
                echo
                echo "Unable to determine the mode"
                echo
            fi
        fi
    else
        echo
        echo "No Pid file"
    fi
}

start_normal() {
    if [ -f $PID ]
    then
        echo
        echo "Already started. PID: [$( cat $PID )]"
    else
        echo "==== Start in normal mode"
        touch $PID
        touch $NORMAL
        if [ -f $ABNORMAL ]
        then
            /bin/rm $ABNORMAL
        fi
        if nohup $COMMAND_NORMAL >>$LOG 2>&1 &
        then echo $! >$PID
             echo "Done."
             echo "$(date '+%Y-%m-%d %X'): START" >>$LOG
        else echo "Error... "
             /bin/rm $PID
             /bin/rm $NORMAL
        fi
    fi
}

start_abnormal() {
    if [ -f $PID ]
    then
        echo
        echo "Already started. PID: [$( cat $PID )]"
    else
        echo "==== Start in abnormal mode"
        touch $PID
        touch $ABNORMAL
        if [ -f $NORMAL ]
        then
            /bin/rm $NORMAL
        fi
        if nohup $COMMAND_ABNORMAL >>$LOG 2>&1 &
        then echo $! >$PID
             echo "Done."
             echo "$(date '+%Y-%m-%d %X'): START" >>$LOG
        else echo "Error... "
             /bin/rm $PID
             /bin/rm $ABNORMAL
        fi
    fi
}

#kill_cmd() {
#    SIGNAL=""; MSG="Killing "
#    while true
#    do
#        LIST=`ps -ef | grep -v grep | grep $CMD | grep -w $USR | awk '{print $2}'`
#        if [ "$LIST" ]
#        then
#            echo; echo "$MSG $LIST" ; echo
#            echo $LIST | xargs kill $SIGNAL
#            sleep 2
#            SIGNAL="-9" ; MSG="Killing $SIGNAL"
#            if [ -f $PID ]
#            then
#                /bin/rm $PID
#            fi
#        else
#           echo; echo "All killed..." ; echo
#           break
#        fi
#    done
#}

stop() {
    echo "==== Stop"

    if [ -f $PID ]
    then
        if kill $( cat $PID )
        then echo "Done."
             echo "$(date '+%Y-%m-%d %X'): STOP" >>$LOG
        fi
        /bin/rm $PID
        if [ -f $NORMAL ]
        then
            /bin/rm $NORMAL
        fi
        if [ -f $ABNORMAL ]
        then
            /bin/rm $ABNORMAL
        fi
        #kill_cmd
    else
        echo "No pid file. Already stopped?"
    fi
}

switch() {
    if [ -f $PID ]
    then
        if [ -f $NORMAL ]
        then
            echo "==== Switch from normal to abnormal"
            stop ; echo "Sleeping..."; sleep 1 ;
            start_abnormal
        else 
            echo "==== Switch from abnormal to normal"
            stop ; echo "Sleeping..."; sleep 1 ;
            start_normal
        fi
    else
        echo "You need to run the simulator before switching"
    fi
}

case "$1" in
    'start')
            start_normal
            ;;
    'stop')
            stop
            ;;
    'restart')
            stop ; echo "Sleeping..."; sleep 1 ;
            start_normal
            ;;
    'status')
            status
            ;;
    'switch')
            switch
            ;;
    *)
            echo
            echo "Usage: $0 { start | stop | restart | status | switch }"
            echo
            exit 1
            ;;
esac

exit 0
