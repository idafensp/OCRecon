import rdflib
import pandas as pd
import oc_api as oca
from SPARQLWrapper import SPARQLWrapper, JSON

import logging
logger = logging.getLogger(__name__)

#NOTES
#https://stackoverflow.com/questions/15727420/using-python-logging-in-multiple-modules

# esta_libs.py

def save_to_file(file, list):
	logger.info("Writing %s lines to file %s" % (len(list),file))
	
	thefile = open(file, 'w')

	for item in list:
		thefile.write("%s\n" % item)


def get_subclassesof(ifile, top_class_uri, tree=True):
	# http://rdflib.readthedocs.io/en/stable/intro_to_sparql.html
	g = rdflib.Graph()

	# ... add some triples to g somehow ...

	g.parse(ifile)


	orgClass = rdflib.term.URIRef(top_class_uri)

	class_list  = [orgClass]

	if tree:
		stack  = [orgClass]
		while stack:
			currentClass = stack.pop()
			logger.debug("c>")
			logger.debug(currentClass)
			subclasses = [s for s, p, o in g if p == rdflib.RDFS.subClassOf and o == currentClass]
			if subclasses:
				stack  =  stack + subclasses
				class_list = class_list + subclasses
				#print(subclasses)


	class_list = set(class_list)
	return class_list


def get_individuals_and_labels(endpoint, type_class, pagination=10000):	
	#SQL wrapper documentation

	paginate = True
	results_df = pd.DataFrame(columns=['Individual','RDFS label','FOAF name'])
	counter = 0
	offset = 0

	while paginate:

		# pagination with scrolling, so Virtuoso does not complain
		query = """
			SELECT DISTINCT ?ind ?label ?fname	
			WHERE 
			{ 
				{ 
					SELECT DISTINCT ?ind ?label ?fname	
					WHERE 
					{
	    				?ind a <"""+type_class+"""> .
	    				OPTIONAL { ?ind rdfs:label ?label }
	    				OPTIONAL { ?ind foaf:name ?fname }
					}
					ORDER BY  ?ind ?label ?fname
				} 
			}
			OFFSET """+str(offset)+""" 
			LIMIT """+str(pagination)+"""
		"""

		logger.debug(query)

		sparql = SPARQLWrapper(endpoint)
		sparql.setQuery(query)


		sparql.setReturnFormat(JSON)
		results = sparql.query().convert()

		logger.info("Got %s results" % len(results["results"]["bindings"]))

		for result in results["results"]["bindings"]:
			ind = result["ind"]["value"]
			#print(ind)

			label = ""
			if "label" in result:
				label = result["label"]["value"]
				#print(label)

			fname = ""
			if "fname" in result:
				fname = result["fname"]["value"]
				#print(fname)

			results_df = results_df.append({
				"Individual": ind,
				"RDFS label": label,
				"FOAF name": fname
			}, ignore_index=True)

			counter+=1
			if(counter % 1000 == 0):
				logger.info("Queried %s results" % counter)

		if(len(results["results"]["bindings"]) < pagination):
			paginate = False #end of pagination
		else:
			offset += pagination #update pagination index
	
	logger.info("Queried total %s results" % counter)
	return results_df


def df_to_csv(file, df):
	df.to_csv(file, sep='\t', encoding='utf-8')


def orgs_to_csv(csv_path):
	class_list = get_subclassesof('./files/dbpedia_3.9.owl', 'http://dbpedia.org/ontology/Organisation', False)

	logger.info("Got %s classes"  % len(class_list))

	save_to_file('dbo_organisation_tree.txt', class_list)


	df_list = []
	for cl in class_list:
	 	df = get_individuals_and_labels('http://es.dbpedia.org/sparql', cl)
	 	df_list.append(df)

	logger.info("Got %s d.frames"  % len(df_list))

	df_labels = pd.concat(df_list)

	df_to_csv(csv_path, df_labels)


def get_owl_same_as(label, uri, graph, get_fisrt, thresh):
	logger.debug("Checking label %s, for res %s" % (label, uri))
	res_list = oca.get_recon_entries(label, get_fisrt, thresh)
	logger.debug("Got entries: %s" % (res_list))

	uri_ind = rdflib.URIRef(uri)
	for recon in res_list:
		osa_ind = rdflib.URIRef(recon['uri'])
		logger.debug("OWL SAME AS: Adding <%s,owl:sameAs,%s>" % (uri_ind, osa_ind))
		graph.add((uri_ind, rdflib.OWL.sameAs, osa_ind))



