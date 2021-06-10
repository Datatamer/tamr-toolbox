"""Snippet showing how the logger module of tamr_toolbox works"""
import tamr_toolbox as tbox

# Often I want to log things as my script progresses - to do this you need a logger object
# Tamr-toolbox will help you put all of your logs in the same place by giving you simple methods
# for creating and managing both your custom loggers, logging from your custom modules, logging
# from the toolbox modules themselves, and loggers from any python libraries that follow best
# practices. By default Tamr-toolbox logging will stream all log messages to console, but it is
# easy enough to have them all go to a file like so.
log = tbox.utils.logger.create("my_log", log_directory=".")

# By default, the logger will still log to the console when a directory is specified, but
# you can disable this and just log to a file by doing
log = tbox.utils.logger.create("my_log", log_to_terminal=False, log_directory=".")

# now I can log whatever I wish. The default log level is INFO so this
log.debug("a debug message")
# won't show up, but this
log.info("an info message")
# will be written to a file with today's date in the name

# note that by default the logging from Tamr-toolbox won't show up, however you can have it
# written to the same log file by simply calling
tbox.utils.logger.enable_toolbox_logging(log_directory=".")

# you can extend to any module/library you use in your code, including any custom modules in the
# same codebase as your script, via the helpful enable_package_logging method
# for example with the tamr unify client
tbox.utils.logger.enable_package_logging("tamr_unify_client")

# if you need debug for any of your logs that is easy enough, just use the set_logging_level
# and pass the name of the logger who's level you want to set
tbox.utils.logger.set_logging_level("my_log", "debug")
log.debug("a debug message that shows up")

# You can use this to set different levels for different packages, for example if you want the
# nitty-gritty of what is happening in the toolbox, but don't want to see all of your code's
# debug statements simply do
tbox.utils.logger.set_logging_level("my_log", "warning")
tbox.utils.logger.set_logging_level("tamr_toolbox", "debug")
