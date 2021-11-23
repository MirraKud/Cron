from crontab import CronTab
from croniter import croniter
from datetime import datetime

import os
import signal
import sys

import logging

from config import *

def getJobsList(tab_file_name):
    """Crontab file parser"""
    try:
        cron = CronTab(tabfile=tab_file_name)

        jobs_list = []

        datetime_to_start_cron = datetime.now().timestamp()

        for job in cron:

            job_time_in_croniter = croniter(str(job.slices), datetime_to_start_cron, is_prev=True)

            jobs_list.append([job_time_in_croniter, job.command])

        return jobs_list
    except Exception as exc:
        logging.error("Error:{}".format(e))
        sys.exit(1)

def runCommand(command):
    return_value = 3
    try:
        return_value = os.system(command)
        logging.info("Succesfuly executed command: '{}'".format(command))
    except Exception as e:
        logging.error("Error:{}, CommandToExecute: '{}'".format(e, command))
    finally:
        sys.exit(return_value)

def workflow(jobs_list):

    if len(jobs_list) == 0:
        logging.info("Nothing to execute. Check if there is any commands and their spelling is right. End of program")
        sys.exit(0)

    while True:
        time_now = datetime.now().timestamp()
        for i in range(len(jobs_list)):

            if jobs_list[i][0].get_current() <= time_now:

                jobs_list[i][0].get_next()

                pid = os.fork()

                if pid > 0:
                    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
                    continue
                else:
                    runCommand(jobs_list[i][1])

if __name__ == "__main__":
    config = CONFIGURATION_INFO

    jobs_list = getJobsList(config["CrontabFileName"])

    logging.basicConfig(filename="logs.log",
                        level=config["LoggingLevel"],
                        format="[%(asctime)s]:%(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %I:%M:%S")

    logging.info("Start of program")
    workflow(jobs_list)



