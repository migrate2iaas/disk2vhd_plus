# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2021 Migrate2Iaas"
#---------------------------------------------------------

import sys



import platform
import shutil
import os
import stat
import ConfigParser
import logging
import codecs
import time
import traceback
from MigrateConfig import VolumeMigrateConfig
import UnicodeConfigParser
import GzipChunkMedia
import GzipChunkMediaFactory
import RawGzipMediaFactory
import datetime
import StreamVmdkMediaFactory
import SparseRawMediaFactory
import QemuImgMediaFactory
import VhdQcow2MediaFactory
import SparseRawMedia



class VolumeMigrateNoConfig(VolumeMigrateConfig):
    def __init__(self, volumename, imagepath , imagesize):
        super(VolumeMigrateNoConfig, self).__init__()
        self.__volumeName = volumename
        self.__imagePath = imagepath
        self.__imageSize = imagesize
        self.__pathToUpload = ''
        self.__uploadedImageId = ''
        self.__excludedDirs = list()
        self.__system = False

    def getImagePath(self):
        return self.__imagePath

    def getUploadPath(self):
        return  self.__pathToUpload 

    def getUploadId(self):
        return self.__uploadedImageId

    def getImageSize(self):
        return self.__imageSize

    def getVolumePath(self):
        return self.__volumeName

    def getExcludedDirs(self):
        return self.__excludedDirs

    def setUploadPath(self, path):
        self.__pathToUpload = path

    def setUploadId(self , uploadid):
        self.__uploadedImageId = uploadid

    def setImagePath(self , imagepath):
        self.__imagePath = imagepath

    def setImageSize(self , size):
        self.__imageSize = size
   
    def saveConfig(self):
        return

    def generateMigrationId(self):
        return

    def isSystem(self):
        return self.__system

    def setSystem(self , system_flag):
        self.__system = system_flag

class VolumeMigrateIniConfig(VolumeMigrateConfig):
    """ volume migration parms got from ini file """

    #NOTE: really , there are just two functions to override: load config and save config
    # the common code should be moved to base class then
    def __init__(self, config, configname , section, volumename):
        super(VolumeMigrateIniConfig, self).__init__()
        self.__config = config
        self.__section = section
        self.__configFileName = configname
        self.__volumeName = volumename
        self.__imagePath = ''
        self.__imageSize = 0
        self.__pathToUpload = ''
        self.__uploadedImageId = ''
        self.__excludedDirs = list()
        self.__system = False

        if config.has_section(section):
            if config.has_option(section, 'imagesize'):
                self.__imageSize = config.getint(section, 'imagesize')
                logging.debug("imagesize for volume " + str(self.__volumeName) + " is pre-set to " + str(self.__imageSize))
            else:
                logging.debug("imagesize was not found in the config for volume " + str(self.__volumeName)) 

            if config.has_option(section, 'pathuploaded'):
                self.__uploadedImageId = config.get(section, 'pathuploaded')
            else:
                logging.debug("pathuploaded was not found in the config for volume " + str(self.__volumeName)) 

            if config.has_option(section, 'pathtoupload'):
                self.__pathToUpload = config.get(section, 'pathtoupload')
            else:
                logging.debug("pathtoupload was not found in the config for volume " + str(self.__volumeName)) 

            if config.has_option(section, 'imagepath'):
                self.__imagePath = config.get(section, 'imagepath')
            else:
                logging.debug("imagepath was not found in the config for volume " + str(self.__volumeName)) 

            if config.has_option(section, 'system'):
                self.__system = config.getboolean(section, 'system')
            else:
                logging.debug("system was not found in the config for volume " + str(self.__volumeName)) 

            # excludedir is a string of dirs separated by ;
            dirstr = ''
            if config.has_option(section, 'excludedir'):
                dirstr = config.get(section, 'excludedir') + ';'

            # Add additional auto exclude dirs here
            if config.has_option(section, 'autoexclude'):
                if str(config.get(section, 'autoexclude')).lower() == "true":
                    dirstr += "\\$RECYCLE.BIN;\\System Volume Information"
                    # Add auto exclude for cloudscraper images path for given volume
                    if config.has_option('Image', 'image-dir'):
                        image_dir = config.get('Image', 'image-dir')
                        if str(self.__volumeName).endswith(image_dir[:2]):
                            dirstr += ";" + image_dir[2:]

            logging.info("excludedirs " + str(dirstr) + " for volume " + str(self.__volumeName))
            if dirstr:
                self.__excludedDirs = dirstr.split(";")
        else:
            logging.warn("! Section for drive letter cannot be found") 
            return

    def getSection(self):
        return self.__section

    def getImagePath(self):
        return self.__imagePath

    def getUploadPath(self):
        return  self.__pathToUpload 

    def getUploadId(self):
        return self.__uploadedImageId

    def getImageSize(self):
        return self.__imageSize

    def getVolumePath(self):
        return self.__volumeName

    def getExcludedDirs(self):
        return self.__excludedDirs

    def setUploadPath(self, path):
        self.__pathToUpload = path

    def setUploadId(self , uploadid):
        self.__uploadedImageId = uploadid

    def setImagePath(self , imagepath):
        self.__imagePath = imagepath
   
    # image size here is the size of volume in bytes (not in the image file that could be compressed)
    def setImageSize(self , size):
        self.__imageSize = size
        logging.debug("imagesize for volume " + str(self.__volumeName) + " is set to " + str(self.__imageSize))
        #logging.debug(str(traceback.format_tb()))

    def isSystem(self):
        return self.__system

    def setSystem(self , system_flag):
        self.__system = system_flag

    def generateMigrationId(self):
        """generates an id to distinguish migration of the same volumes but for different times"""
        return (os.environ["COMPUTERNAME"] + "_" + datetime.date.today().strftime("%Y_%m_%d") + "_" + str(self.getVolumePath())).replace("\\" , "").replace("." , "_").replace(":" , "")

    def saveConfig(self):
        section = self.__section
        if self.__config.has_section(section) == False:
            self.__config.add_section(section)
        
        if self.__imageSize:
            self.__config.set(section, 'imagesize' , str(self.__imageSize))

        if self.__uploadedImageId:
            self.__config.set(section, 'pathuploaded' , self.__uploadedImageId)

        if self.__pathToUpload:
            self.__config.set(section, 'pathtoupload' , self.__pathToUpload)

        if self.__imagePath:
           self.__config.set(section, 'imagepath', self.__imagePath)

        if self.__system:
           self.__config.set(section, 'system', self.__system)

        fconf = codecs.open(self.__configFileName, "w", "utf16")#file(self.__configFileName, "w")
        self.__config.write(fconf)


class MigratorConfigurer(object):
    """ This class is up to make configs for various cloud migrations"""

    def __init__(self):
        return

    #automcatically chooses which cloud to generate the config for
    def configAuto(self , configfile):
        try:
            config = UnicodeConfigParser.UnicodeConfigParser()
            config.readfp(codecs.open(configfile, "r", "utf16"))
        except Exception as e:
            logging.info("Couldn't read an config as unicode file due to " + str(e) + " . Reading as ascii")
            config = ConfigParser.RawConfigParser()
            config.read(configfile)
        logging.debug("Config read:" + repr(config))
        return self.configVols(configfile , config)

    def configVols(self , configfile, config):
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config , configfile, imagedir, imagetype)        
        factory = self.createImageFactory(config , image_placement , imagetype)
        return (image, adjust_override, None)


    def getOverrides(self, config , configfile):
        """returns dict() of overrides"""
        #override copy-paste
        adjust_override = dict()
        if config.has_section('Fixes'):
            # not very nice but should work
            adjust_override.update(config._sections['Fixes'])

        return adjust_override

    def createVolumesList(self , config, configfile, imagedir , imagetype , upload_prefix = ""):
        """creates volume list"""
        volumes = list()
        if config.has_section('Volumes') :
            
            #check what volumes to migrate
            letters=""
            letterslist = list()
            if config.has_option('Volumes', 'letters'):
                letters = config.get('Volumes', 'letters')                 
            letterslist = letters.split(',')

            # if system is set , add autolocated system volume by default
            if config.has_option('Volumes', 'system'):
                addsys = config.getboolean('Volumes', 'system') 
                if addsys:
                    logging.debug("system is set in volumes config")
                    sysvol = os.environ['windir'].split(':')[0] #todo: change to cross-platform way. windir is set artificially at the program start for linux
                    if not sysvol in letterslist:
                        letterslist.append(sysvol)
                        logging.debug("appending system volume " + sysvol + " to the volume list")
                    else:
                        logging.debug("skipping  " + sysvol + " - it is already in the list")

            logging.debug("Volume letters to process are: " + str(letterslist))
            for letter in letterslist:
                letter = str(letter).strip() #remove spaces between commas
                if not letter:
                    continue
                letter = str(letter).strip()
                if os.name == 'nt':
                    devicepath = '\\\\.\\'+letter+':'
                    sys.path.append('./Windows')
                    try:
                        import Windows
                        size = Windows.Windows().getSystemInfo().getVolumeInfo(letter+":").getSize()
                    except Exception as e:
                        logging.debug("Cannot get local data on volume " + letter + " " + str(e))
                        size = 0
                else:
                    if not "/dev/" in letter: 
                        devicepath = "/dev/"+letter
                    else:
                        devicepath = letter
                    sys.path.append('./Linux')
                    try:
                        import Linux
                        size = Linux.Linux().getSystemInfo().getVolumeInfo(devicepath).getSize()
                    except Exception as e:
                        logging.debug("Cannot get local data on volume " + letter + " " + str(e))
                        size = 0
                volume = VolumeMigrateIniConfig(config , configfile , letter , devicepath)
                if volume.getImagePath() == '':
                    volume.setImagePath(imagedir+"/"+letter+"."+imagetype);
                if volume.getImageSize() == 0:
                    volume.setImageSize(size)
                if volume.getUploadPath() == '':
                    volume.setUploadPath(upload_prefix+os.environ['COMPUTERNAME']+"-"+letter)
                volumes.append( volume )
        return volumes

    def createImageFactory(self , config , image_placement , imagetype):
        """generates factory to create media (virtual disk files) to store image before upload"""
        compression = 2
        if config.has_option('Image', 'compression'):
            compression =  config.getint('Image', 'compression')
        
        factory = None
        if (imagetype == "VHD" or imagetype == "fixed.VHD") and image_placement == "local":
            # check run on windows flag
            if os.name == 'nt':
                import WindowsVhdMediaFactory
                factory = WindowsVhdMediaFactory.WindowsVhdMediaFactory(fixed = (imagetype == "fixed.VHD"))
            else:
                factory = QemuImgMediaFactory.QemuImgMediaFactory()    
        
        if (imagetype == "raw.tar" or imagetype.lower() == "raw") and image_placement == "local":
            chunk = 4096*1024
            factory = GzipChunkMediaFactory.GzipChunkMediaFactory(chunk , compression)
        if (imagetype == "stm.vmdk" or imagetype.lower() == "vmdk") and image_placement == "local":
            factory = StreamVmdkMediaFactory.StreamVmdkMediaFactory(compression) 
        if (str(imagetype).lower() == "sparsed" or imagetype.lower() == "sparsed.raw"):
            factory = SparseRawMediaFactory.SparseRawMediaFactory()

        #Here we can do some additional conversation using qemu utilities
        if (config.has_option('Qemu', 'path') and config.has_option('Qemu', 'dest_imagetype')):
            qemu_path = config.get('Qemu', 'path')
            dest_imagetype = config.get('Qemu', 'dest_imagetype')
            qemu_convert_params = ""
            if config.has_option('Qemu', 'qemu_convert_params'):
                qemu_convert_params = int(config.get('Qemu', 'qemu_convert_params'))
            factory = VhdQcow2MediaFactory.VhdQcow2MediaFactory(factory , qemu_path , dest_imagetype , qemu_convert_params = qemu_convert_params)

        return factory

    def getImageOptions(self , config):
        """gets tuple of image related data (image placement , image types , image path (directory) ) """
        imagearch = config.get('Image', 'source-arch')

        if config.has_option('Image', 'image-type'):
            imagetype = config.get('Image', 'image-type')
        else:
            imagetype = 'raw.gz'
            logging.warning("No image type specified. Default raw.gz is used.");

        imagedir = ""
        if config.has_option('Image', 'image-dir'):
           imagedir = config.get('Image', 'image-dir') 
        else:
            imagedir = "."
            logging.warning("No directory for image store is specified. It'll be created in local script execution directory");

        if imagedir[-1] == '\\':
            imagedir = imagedir[0:-1]
        if os.path.exists(imagedir) == False:
            logging.debug("Directory " + unicode(imagedir) + " not found, creating it");
            os.mkdir(imagedir)           
        else:
            dirmode = os.stat(imagedir).st_mode
            if stat.S_ISDIR(dirmode) == False:
                #TODO: create wrapper for error messages
                #TODO: test UNC path
                logging.error("!!!ERROR Directory given for image storage is not valid!") 

        image_placement = ""
        if config.has_option('Image', 'image-placement'):
           image_placement = config.get('Image', 'image-placement') 
        else:
           image_placement = "local"

        return (imagedir, image_placement, imagetype)


