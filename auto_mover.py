"""Python daemon to watch a directory and copy new files to another directory.

REQUIRED PACKAGES:
 - watchdog
 - daemonize
"""
from daemonize import Daemonize
import logging
import os
import re
import subprocess
import sys
from time import sleep
from watchdog.observers import Observer  
from watchdog.events import PatternMatchingEventHandler 


VAULT_101 = "/mnt/nfs/Vault-101"
VAULT_102 = "/mnt/nfs/Vault-102"

DIR_RUN = os.path.join(VAULT_102, "scripts")
DIR_WATCH = os.path.join(VAULT_102, "downloaded")
DIR_COMPLETE = os.path.join(VAULT_101, "NEW")
MOVIE_DIR = os.path.join(VAULT_101, "MOVIES")
PID = os.path.join(DIR_RUN, "auto_mover.pid")
LOG_FILE = os.path.join(DIR_RUN, "auto_mover.log")

EXT_LIST = [".avi", ".mkv", ".rar"]
IGNORE_LIST = ["sample", "preview", "sub", "subs"]
IGNORE_RE =  "(?:^|[\d\W_]){}(?:$|[\d\W_])"
MOVIE_RE = "(\w.+?)\W+?((?:19|20)\d{2})"

class FileHandler(PatternMatchingEventHandler):
  """This creates the event handler for new files we are interested in."""
  
  patterns = ["*{}".format(ext) for ext in EXT_LIST]

  def process(self, event):
    ext = os.path.splitext(event.src_path)[1]
    if ext not in [".rar"]:
      auto_move(event.src_path)
    else:
      auto_unrar(event.src_path)

  def on_created(self, event):
    self.process(event)


def is_movie_check(file_name, pattern=MOVIE_RE):
  is_movie = re.search(pattern, file_name)
  if is_movie:
    return (is_movie.group(1), is_movie.group(2)) # (movie_title, movie_year) 
  return None


def clean_file_name(file_name):
  dirty = ['_', '.', ',']
  for each in dirty:
    file_name = file_name.replace(each, " ")
  return file_name


def ignore_check(file_name, ignore_list=IGNORE_LIST):
  """Checks if filename contains a sub-string we are not interested in."""
  for ignore in ignore_list:
    regex = IGNORE_RE.format(ignore)
    if re.search(regex, file_name, re.IGNORECASE):
      return True
  return False

def auto_unrar(file_path, file_dest=DIR_WATCH):
  command = "unrar e '{}' '{}'".format(file_path, file_dest)
  logger.info("RAR Found: {}".format(file_path))
  try:
    logger.info("Extracting...")
    logger.debug("To: {}".format(file_dest))
    subprocess.check_output(command, shell=True)
  except Exception as exception:
        logger.error("Error trying to run: {}".format(command))
        logger.error(exception)

def auto_move(file_path):
  file_path = os.path.abspath(file_path)
  logger = logging.getLogger('AutoMover')
  file_name = os.path.basename(file_path)
  logger.info("New File Found: {}".format(file_name))
  ignore = ignore_check(file_name)
  if not ignore: # Passes ignore check 
    file_check = os.path.join(DIR_COMPLETE, file_name)
    movie_check = is_movie_check(file_name)
    if movie_check:
      movie_title = clean_file_name(movie_check[0])
      movie_year = movie_check[1]
      logger.info("Found Movie: {} {}".format(movie_title, movie_year))
      make_movie(file_path, movie_title, movie_year)
    elif not os.path.isfile(file_check): # Check if file already exists
      command = "cp '{}' {}".format(file_path, DIR_COMPLETE)
      logger.info("Executing: {}".format(command))
      try:
        subprocess.check_output(command, shell=True)
        logger.info("Command Successfull")
      except Exception as exception:
        logger.error("Error trying to run: {}".format(command))
        logger.error(exception)
    else:
      logger.warning("Skipping {}".format(file_name))
      logger.debug("File already exists: {}".format(file_check))
  else:
    logger.warning("Skipping {}".format(file_name))
    logger.debug("Ignore string: {}".format(ignore))


def make_movie(file_path, movie_name, movie_year):
  logger = logging.getLogger('AutoMover')
  movie_title = movie_name
  if movie_title[:4].lower() == "the ":
    movie_title = movie_title[4:] + ", The"
  if movie_title[:2].lower() == "a ":
    movie_title = movie_title[2:] + ", A"
  file_ext = os.path.splitext(file_path)[1]
  movie_title = movie_title + ' (' + movie_year + ')'
  movie_dir = os.path.join(MOVIE_DIR, movie_title)
  dest_file_path =  os.path.join(movie_dir, movie_name + file_ext)
  mkdir_command = "mkdir -m 755 '{}'".format(movie_dir)
  acl_command = "chown nobody:users '{}'"
  mv_command = " mv '{}' '{}'".format(file_path, dest_file_path)
  logger.info("Moving Files for {}".format(movie_name))
  logger.debug("From: {}".format(file_path))
  logger.debug("To: {}".format(dest_file_path))
  # Create a new movie directory if it doesn't exist.
  if not os.path.isdir(movie_dir):
    try:
      acls = acl_command.format(movie_dir)
      logger.info("Creating directory")
      logger.debug(mkdir_command)
      subprocess.check_output(mkdir_command, shell=True)
      logger.info("Setting permisions")
      logger.debug(acls)
      subprocess.check_output(acls, shell=True)
    except Exception as e:
      logger.error("Error executing commands")
      logger.debug(e)
  else:
    logger.warning("Already Exists: {}".format(movie_dir))
  # Move and rename movie file to new movie directory if it doesn't exist.
  if not os.path.isfile(dest_file_path):
    try:
      acls = acl_command.format(dest_file_path)
      logger.info("Moving Files")
      subprocess.check_output(mv_command, shell=True)
      logger.info("Setting permisions")
      logger.debug(acls)
      subprocess.check_output(acls, shell=True)
    except Exception as e:
      logger.error("Error executing commands")
      logger.debug(e)
  else:
    logger.warning("Already Exists: {}".format(dest_file_path))
  logger.info("Done!")


def start_watcher():
  logger = logging.getLogger('AutoMover')
  logger.warning("=========AUTO-MOVER HAS STARTED=========")
  # Setup watcher
  file_handler = FileHandler()
  file_observer = Observer()
  file_observer.schedule(file_handler, DIR_WATCH, recursive=True)
  # Start watching
  file_observer.start()
  try:
    while True:
      sleep(1)
  except KeyboardInterrupt:
    file_observer.stop()
  file_observer.join()

  
def scan_dir(path_to_scan=DIR_WATCH):
  logger = logging.getLogger('AutoMover')
  logger.warning("==========MANUAL SCAN INITIATED==========")
  for root, dirs, files in os.walk(path_to_scan):
    for name in files:
      file_path = os.path.join(root, name)
      extension = os.path.splitext(name)[1]
      if extension in EXT_LIST:
        auto_move(file_path)


if __name__ == "__main__":
  # Setup Logging
  logger = logging.getLogger('AutoMover')
  handler = logging.FileHandler(LOG_FILE)
  formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
  handler.setFormatter(formatter)
  logger.addHandler(handler) 
  logger.setLevel(logging.DEBUG)
  keep_fds = [handler.stream.fileno()]
  
  if "scan" in sys.argv:
    # Log to stdout
    scan_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(scan_handler)
    if len(sys.argv) > 2:
      tmp_path = os.path.abspath(sys.argv[2])
      if os.path.isdir(tmp_path):
        print "Scanning: {}".format(tmp_path)
        scan_dir(tmp_path)
      else:
        print "Invalid Path: {}".format(tmp_path)
        sys.exit()
    else:
      print "Scanning: {}".format(DIR_WATCH)
      scan_dir()
    
  if "bg" in sys.argv:
    print "Running in background"
    pid = PID
    daemon = Daemonize(app="auto_mover", pid=pid,
                       action=start_watcher, keep_fds=keep_fds)
    daemon.start()
    
