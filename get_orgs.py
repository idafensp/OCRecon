import esta_libs as esl
import esta_utils as esu
import oc_api as oca
import pandas as pd
import logging as logger
import sys
import rdflib
import time


def main():
	logger.info("%s arguments: %s" % (len(sys.argv), sys.argv))

	step = 1
	start_row = 0
	end_row = -1
	csv_path = "./res_labels.tsv"
	lab_flags = "RF"
	check_rdfs_labels = False
	check_foaf_name = False
	threshold = 0
	get_first = True
	MAX_COUNT = -1
	PROGRESS_COUNT = 10


	if len(sys.argv) > 1:
		step = int(sys.argv[1])
		
	if len(sys.argv) > 2:
		csv_path = sys.argv[2]

	if len(sys.argv) > 3:
		lab_flags = sys.argv[3]

	if len(sys.argv) > 4:
		PROGRESS_COUNT = int(sys.argv[4])

	if len(sys.argv) > 5:
		MAX_COUNT = int(sys.argv[5])

	if len(sys.argv) > 6:
		start_row = int(sys.argv[6])

	if len(sys.argv) > 7:
		end_row = int(sys.argv[7])

	if len(sys.argv) > 8:
		get_first = False
		threshold = float(sys.argv[8])
	
	check_rdfs_labels ='R' in lab_flags
	check_foaf_name ='F' in lab_flags


	logger.info("Starting at step %s" % (step))
	logger.info("CSV path: %s" % (csv_path))
	logger.info("lab_flags: %s, rdfs labels?=%s, foaf names?=%s" % (lab_flags,check_rdfs_labels, check_foaf_name))
	logger.info("MAX_COUNT: %s" % (MAX_COUNT))
	logger.info("PROGRESS_COUNT: %s" % (PROGRESS_COUNT))
	logger.info("Starting row %s" % (start_row))
	logger.info("Ending row %s" % (end_row))
	logger.info("threshold: %s, get_first?=%s" % (threshold,get_first))

	time0 = time.time()

	if step < 1:
		logger.info("Starting orgs_to_csv")
		esl.orgs_to_csv(csv_path)




	time1 = time.time()
	logger.info("Step 1 time = %s" % (time1-time0))

	count = 0;
	osa_graph = rdflib.Graph()
	if step < 2:
		df = pd.read_csv(csv_path, sep='\t')

		if end_row<0:
			end_row = len(df)

		df = df[start_row:end_row]

		logger.debug("Got %s rows from %s" % (csv_path, len(df)))
		for tup in df.itertuples():
			uri = tup[2]
			label = tup[3]
			fname = tup[4]

			# check RDFS labels if necessary
			if check_rdfs_labels and pd.notnull(label):
				logger.info("Reconciling RDFS label %s" % (label))
				esl.get_owl_same_as(label, uri, osa_graph, get_first, threshold)

			# if RDFS label is the same as FOAF name, no need to check
			if pd.notnull(label) and  pd.notnull(fname) and fname == label:
				continue

			# check FOAF names if necessary
			if check_foaf_name and pd.notnull(fname):
				logger.info("Reconciling FOAF name %s" % (fname))
				esl.get_owl_same_as(fname, uri, osa_graph, get_first, threshold)
			
			count +=1

			if esu.count_progress(count, len(df), osa_graph, MAX_COUNT, PROGRESS_COUNT):
				break


	time2 = time.time()
	logger.info("Step 2 time = %s" % (time2-time1))


	logger.info("A total of %s owl:sameAs links have been generated, writing to file" % (len(osa_graph)))
	osa_graph.serialize(destination=csv_path+'.nt', format='nt')


	time3 = time.time()
	logger.info("Writing owl:sameAs time = %s" % (time3-time2))


	logger.info("Step 1 time = %s" % (time1-time0))
	logger.info("Step 2 time = %s" % (time2-time1))
	logger.info("Writing owl:sameAs time = %s" % (time3-time2))


#change this to file config
if __name__ == '__main__':
	import logging.config
	logging.basicConfig(filename='log_get_orgs.log', format='%(asctime)s %(levelname)s %(message)s', level=logger.DEBUG)
	main()
