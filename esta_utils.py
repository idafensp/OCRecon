import time
import logging
logger = logging.getLogger(__name__)

def count_progress(count, total, graph, MAX_COUNT,PROGRESS_COUNT, gotosleep=True):
	if (count % PROGRESS_COUNT) == 0:
		logger.info("%s/%s reconciliation attempts, got %s hits so far" % (count, total, len(graph)))
		if(gotosleep):
			sleep()

	if (MAX_COUNT > 0) and (count > MAX_COUNT-1):
		logger.info("Max count reached, exiting ater %s iterations and %s hits" % (count,len(graph)))
		return True

def sleep(secs = 3):
	logger.info("Sleeping for %s seconds..." % (secs))
	time.sleep(secs)
	logger.info("... back on track")
