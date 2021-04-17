# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2021 Migrate2Iaas"
#---------------------------------------------------------

import sys
import unittest

import AdjustedBackupSource
import BackupAdjust
import Migrator
import SystemAdjustOptions
import MigratorConfigurer
import Version

import getpass
import platform

import logging
import datetime
import traceback
import argparse
import time
import traceback
import random
import errno
import threading 
import os
import md5
import sha
import base64


class ArgumentParserError(Exception): pass

class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)


if os.name == 'nt':
    sys.path.append('./Windows')
    import WindowsVolume
    import WindowsBackupSource

MigrateVerisonHigh = Version.majorVersion
MigrateVersionLow = Version.minorVersion


def heartbeat(interval_sec):
    while 1:
        logging.info(".")
        print('.')
        time.sleep(int(interval_sec))


def chk_ver():
    """checks version , always returns true"""
    return True

def chk_limits():
    """no limits"""
    return 0

if __name__ == '__main__':
    try:
        #parsing extra option
        parser = ThrowingArgumentParser(description="This script performs creation of virtualized images from the local server, uploading them to S3, converting them to EC2 instances. See http://www.migrate2iaas.com for more details.")
        parser.add_argument('-c', '--config', help="Path to copy config file. May be a local path or http link")
        parser.add_argument('-o', '--output', help="Path to extra file for non-detalized output")                   
        parser.add_argument('-u', '--resumeupload', help="Resumes the upload of image already created", action="store_true")                   
        parser.add_argument('-s', '--skipupload', help="Skips both imaging and upload. Just start the machine in cloud from the image given", action="store_true")                   
        parser.add_argument('-t', '--testrun', help="Makes test run on the migrated server to see it responding.", action="store_true") 
        parser.add_argument('-z', '--timeout', help="Specify timeout to wait for test run server to respond", type=int, default=480)                  
        parser.add_argument('-b', '--heartbeat', help="Specifies interval in seconds to write hearbeat messages to stdout. No heartbeat if this flag is ommited", type=int)                   
        parser.add_argument('-v', '--virtio', help="Injects virtio drivers in the running server driver store", action="store_true")
        parser.add_argument('-j', '--reboottimeout', help="Time to wait in seconds for VM to reboot while doing test run", type=int, default=200)
        parser.add_argument('-p', '--backup', help="Backup mode. Skips deploying machine in the cloud - just takes an image and uploads it to cloud", action="store_true")
        parser.add_argument('-l', '--logfile', help="Specifies the place to store full log")

        logfile = "disk2vhd.log"
        if parser.parse_args().logfile:
            logfile = parser.parse_args().logfile
        #Turning on the logging
        logging.basicConfig(format='%(asctime)s %(message)s' , filename=logfile,level=logging.DEBUG)    
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(handler)
    
        #new random seed
        random.seed()
    
        #some legacy command-line support
        if parser.parse_args().output:
            outhandler = logging.FileHandler(parser.parse_args().output , "w" )
            outhandler.setLevel(logging.INFO)
            logging.getLogger().addHandler(outhandler)

        # little hacks to pre-configure env and args
        if os.name == 'nt':
            import Windows
            #converting to unicode, add "CheckWindows" option
            sys.argv = Windows.win32_unicode_argv()
        else:
            print("Non-Windows OS are not supported")
            sys.exit(1)
    
        # starting the heartbeat thread printing some dots while app works
        if parser.parse_args().heartbeat:
            threading.Thread(target = heartbeat, args=(parser.parse_args().heartbeat,) ).start()

        logging.info("\n>>>>>>>>>>>>>>>>> Disk2VHD+ Process ("+ Version.getShortVersionString() + ") is initializing\n")
        logging.info("Full version: " + Version.getFullVersionString())


        config = MigratorConfigurer.MigratorConfigurer()
        # creatiing the config
        if parser.parse_args().config:
            configpath = parser.parse_args().config
        else:
            print()
            #TODO: get parms from cmdline
            #DUMP input to config
            cmdline = 

        testrun = False
        timeout = 0
        reboottimeout = 0
        if parser.parse_args().testrun:
            testrun = True
            timeout = parser.parse_args().timeout
            reboottimeout = parser.parse_args().resumeupload

        backupmode = True            
        skipupload = True

        limits = None
        try:
            #configuring the process
            (image,adjust,cloud) = config.configAuto(configpath , password)
            limits = chk_limits()
        except Exception as e:
            logging.error("\n!!!ERROR: failed to configurate the process! ")
            logging.error("\n!!!ERROR: " + repr(e) )
            logging.error(traceback.format_exc())
            os._exit(errno.EFAULT)
    
        logging.info("\n>>>>>>>>>>>>>>>>> Configuring the Process:\n")
        __migrator = Migrator.Migrator(cloud,image,adjust, resumeupload or skipupload , resumeupload, skipupload , limits = limits , insert_vitio=parser.parse_args().virtio, backup_mode = backupmode)
        logging.info("Started")
        # Doing the task
        instance = None
        error = False
        try:
            instance = __migrator.runFullScenario()
            error = __migrator.getError()
            if (backupmode == True and error == False):  
                logging.info("\n>>>>>>>>>>>>>>>>> Disk2VHD done\n")
        except Exception as e:
            error = True
            logging.error("\n!!!ERROR: Severe error")
            logging.error("\n!!!ERROR: " + str(e) )
            logging.error(traceback.format_exc())

        if error:
               logging.info("\n>>>>>>>>>>>>>>>>>> Process ended unsuccessfully\n")
               #sys.exit(errno.EFAULT)
               os._exit(errno.EFAULT)
        else:
            logging.info("\n>>>>>>>>>>>>>>>>> Process ended successfully\n")
    except Exception as e:
           logging.error("\n!!!ERROR: Unexpected error during init")
           logging.error("\n!!!ERROR: " + str(e) )
           logging.error(traceback.format_exc())
           logging.info("\n!!!ERROR: Process ended unsuccessfully\n")
           os._exit(errno.ERANGE)
    finally:
        os._exit(0)
