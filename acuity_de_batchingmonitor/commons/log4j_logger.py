"""
This module contains a class that wraps the log4j object instantiated by the active SparkContext, enabling Log4j logging for PySpark using.
"""


class Log4j(object):

    def __init__(self, spark):
    
        """
            This method will initilize the log4j logmanager and returns the loger object. Wrapper class for Log4j JVM object

            :param obj spark: The SparkSession object.

            :returns: The Logger object from log4j.

            :rtype: obj
        """
    
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get("spark.app.id")
        app_name = conf.get("spark.app.name")

        log4j = spark._jvm.org.apache.log4j
        message_prefix = "<" + app_name + " " + app_id + ">"
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
    
        """
            It prints messages with the level (Level.ERROR).

            :param obj message: The message object.

            :returns: None.
            
        """

        self.logger.error(message)
        return None

    def warn(self, message):
    
        """
            It prints messages with the level (Level.WARN).

            :param obj message: The message object.

            :returns: None.
            
        """
        
        self.logger.warn(message)
        return None

    def info(self, message):
        
        """
            It prints messages with the level (Level.INFO).

            :param obj message: The message object.

            :returns: None.
            
        """
        
        self.logger.info(message)
        return None

    def debug(self, message):
        
        """
            It prints messages with the level (Level.DEBUG).

            :param obj message: The message object.

            :returns: None.
            
        """
        
        self.logger.debug(message)
        return None
