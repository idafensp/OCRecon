	
# https://opencorporates.com/reconcile/suggest

# https://blog.ouseful.info/tag/opencorporates/?orderby=ID&order=ASC

# request example https://opencorporates.com/reconcile/?query=tesco
# params
# 	query
#	

import requests
from requests.utils import quote
import json
import logging
logger = logging.getLogger(__name__)

#get the list of reconciled entities
#get_first = False, get all entries
#get_first = True, get first entry
#thresh minimun recon score value to return the element
def get_recon_entries(label, get_first=True, thresh=0):
	logger.debug("Reconciling %s, get_first?=%s, thresh=%s" % (label, get_first, thresh))
	encoded_label = quote(label, safe='')
	req_url = 'https://opencorporates.com/reconcile/?query=' + encoded_label

	logger.info(req_url)

	try:
		resp = requests.get(req_url)
	except:
		logger.error("ERROR requesting URL for label %s (%s), request url=%s" % (label,encoded_label, req_url))
		return []

	if resp.status_code != 200:
	    # This means something went wrong.
	    logger.warning("Error processing %s (%s), status %s" % (label, encoded_label, resp.status_code))
	    return []
	else:
		decoded = json.loads(resp.text)

		logger.debug(">>>> Got %s result for %s (%s), status %s" % (len(decoded['result']),label, encoded_label, resp.status_code))

		if len(decoded['result']) < 1 :
			logger.warning("Got No result for %s (%s), status %s" % (label, encoded_label, resp.status_code))
			return []

		if get_first:
			logger.debug("Get First - Score for %s with name %s is %s" % (decoded['result'][0]['uri'],decoded['result'][0]['name'],decoded['result'][0]['score']))
			if decoded['result'][0]['score'] > thresh:
				return [decoded['result'][0]]
			else:
				return []

		return_list = []
		for dec in decoded['result']:
			logger.debug("Score for %s with name %s is %s" % (dec['uri'],dec['name'],dec['score']))
			if dec['score'] >= thresh:
				return_list.append(dec)
			else:
				break

		return return_list

