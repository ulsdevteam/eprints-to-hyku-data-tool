import argparse, csv, json, math, os, re, sys
from datetime import datetime # the datetime people are crazy
import shutil # for zipping
from pytz import timezone
from pathlib import Path
import paramiko # ssh connection
from getpass import getpass # password input
from urllib.parse import unquote # fixing mangled filenames...
import subprocess # to run shell commands??
from ftfy import fix_encoding # encoding fix

# Well, this started out pretty simple and now I look at it and want to refactor it.
# Apologies for the lack of class structure!

DIRECTORY_SEPARATOR = "/"
INPUT_DIRECTORY = "import"
OUTPUT_DIRECTORY = "/mounts/data/hyku/output"
WORKING_DIRECTORY = "/mounts/data/hyku/working"
WORKING_FILES_DIRECTORY = "/mounts/data/hyku/working/files"
DEFINITIONS_DIRECTORY = "definitions"
CATEGORY_FILENAME = "categories.json"
LANGUAGE_CODE_TABLE_FILENAME = "languages.json"

BATCH_DATE_FORMAT = "%Y-%m-%d %H-%M-%S"
EASTERN_TIMEZONE = timezone('US/Eastern')
BATCH_START_TIME = datetime.now(EASTERN_TIMEZONE)
BATCH_NAME = BATCH_START_TIME.strftime(BATCH_DATE_FORMAT)

DOCUMENTS_FILENAME = "files.csv"
DOCUMENTS_METADATA_HEADERS = ['item', 'source_identifier', 'model', 'parents', 'title', 'creator', 'keyword', 'rights', 'license', 'type', 'degree', 'level', 'discipline', 'grantor', 'advisor', 'commitee member', 'department', 'format', 'date', 'contributor', 'description', 'publisher', 'subject', 'language', 'identifier', 'relation', 'source', 'abstract', 'admin_note']

LOGFILE_DIRECTORY = "/mounts/data/hyku/logs"
LOGFILE_DEFAULT = "default.log"
LOGFILE_DEFAULT_ERROR = "error.log"
LOGFILE_DEFAULT_DETAILS = "details.log"
LOGFILE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOGFILE_MISSING_DEGREE_NAME = "missing_degree_name.log"
LOGFILE_MISSING_DEGREE_LEVEL = "missing_degree_level.log"
LOGFILE_MISSING_KEYWORDS = "missing_keywords.log"
LOGFILE_MISSING_RIGHTS = "missing_rights.log"
LOGFILE_FAILED_DOWNLOADS = "failed_downloads.log"
LOGFILE_JSON_CACHE = "json.log"
LOGFILE_EMBARGO = "embargo.log"

# Connect to the source server
ssh_connection = paramiko.SSHClient()
ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Connect to the source server
source_address = "eprints-prod-01"
# get the login info for connecting to the server
source_username = input("Enter your username for connecting to "+source_address+":\n")
print("Enter your password for connecting to "+source_address)
source_password = getpass()

ssh_connection.connect(source_address, username=source_username, password=source_password)
sftp_connection = ssh_connection.open_sftp()
# end connection start

categories = {}

# Load in language codes from an (unfortunately) known file. There are better ways.
# At least I'm starting with an object this time.
class Language_Codes:
	language_table_code_first = {}
	language_table_language_first = {}

	def __init__(self):
		self.load_languages()

	# builds both tables, one by loading in directly with the JSON module, the other by inverting the original table
	def load_languages(self):
		try:
			with open(DEFINITIONS_DIRECTORY+DIRECTORY_SEPARATOR+LANGUAGE_CODE_TABLE_FILENAME, 'r', encoding='utf-8') as f:
				data = json.load(f)
				self.language_table_code_first = data
		except FileNotFoundError:
			print("We can't find the file ["+DEFINITIONS_DIRECTORY+DIRECTORY_SEPARATOR+LANGUAGE_CODE_TABLE_FILENAME+"]. Please make sure it actually  and is accessible by the user executing the script!")
		f.close()

		for language_code in self.language_table_code_first.keys():
			self.language_table_language_first[self.language_table_code_first[language_code]] = language_code


	def get_language_by_code(self, code):
		# dealing with lists
		if type(code) == list:
			if len(code) == 1:
				code = code[0]
			else:
				# oh no we have an actual list of these things
				# might as well recurse over the list right?
				code_list = []
				for one_code in code:
					code_list.append(self.get_language_by_code(one_code))
				return code_list

		if code in self.language_table_code_first.keys():
			return self.language_table_code_first[code]
		else:
			return False

	def get_code_by_language(self, language):
		if language in self.language_table_language_first.keys():
			return self.language_table_language_first[language]
		else:
			return False

def error_to_terminal(error, error_code=1):
	print("\nFatal error!\n\n"+error+"\n\n")
	sys.exit(error_code)

def set_up_directory(dirname):
	our_directory = Path(dirname)
	if our_directory.exists():
		if our_directory.is_dir():
			return
		# Note: I haven't dealt with the condition in which a file exists with the filename of our directory
		# So that could hypothetically result in some unexpected behavior.
	try:
		os.makedirs(dirname)
	except:
		error_to_terminal(f"Error setting up directory: {dirname}")
	return

def log_activity_to_file(logstring="Unknown action.", filename=LOGFILE_DEFAULT):
	local_time = datetime.now(EASTERN_TIMEZONE)
	try:
		with open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+filename, 'a') as output_file:
			output_file.write(local_time.strftime(LOGFILE_DATE_FORMAT)+": "+logstring+"\n")
		output_file.close()
	except:
		error_to_terminal("Error sending log to file.\nLogstring: "+logstring+"\nLog file: "+LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+filename)

def save_json_to_file(json_object, filename, file_encoding='utf-8'):
	dirname = OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+BATCH_NAME
	set_up_directory(dirname)
	try:
		with open(dirname+DIRECTORY_SEPARATOR+filename, 'w', encoding=file_encoding) as output_file:
			json.dump(json_object, output_file, ensure_ascii=False, indent=4)
		output_file.close()
	except:
		error_to_terminal("Error writing JSON to file.\nOutput file: "+dirname+DIRECTORY_SEPARATOR+filename)

def save_csv_to_file(json_object, filename, fieldnames, file_encoding='utf-8'):
	try:
		with open(WORKING_DIRECTORY+DIRECTORY_SEPARATOR+filename, 'w', encoding=file_encoding, newline='\r') as output_file:
			writer = csv.DictWriter(output_file, fieldnames=fieldnames, dialect="excel")

			writer.writeheader()
			for row in json_object:
				writer.writerow(row)
		output_file.close()
	except Exception as err:
		error_to_terminal("Error writing CSV to file.\nOutput file: "+WORKING_DIRECTORY+DIRECTORY_SEPARATOR+filename+f"\n{type(err).__name__} was raised: {err}")

# convert lists to pipe-delimited strings for CSV import
# note that we shouldn't need to add our own quotes
def stringify_list(delimiter, list, escape_character="\\"):
	new_string = ""
	for value in list:
		# Hunt for delimiters in the data that we need to escape
		if delimiter in value:
			value = value.replace(delimiter, escape_character+delimiter)

		# Don't want to start with your delimiter
		if not new_string:
			new_string = str(value)
		else:
			new_string = new_string + delimiter + str(value)
	return new_string

# Take an unsorted list of committee members and sort it by role and then by name
# inside each role
def parse_committee(committee_list):
	# we don't actually know for sure that we don't (or won't) have multiple chairs/cochairs
	# so let's treat all of the roles as multi-fields
	committee = []
	committee_chair = []
	committee_cochair = []
	committee_members = []

	if type(committee_list) == list:
		for committee_member in committee_list:
			temp_member = {}
			temp_member['full-string'] = committee_member

			# if we can't split the string, that means we don't
			#	have a role to sort on. Let's default to committee member.
			#
			# this is, annoyingly, more common than you'd think
			if committee_member.find(" - ") == -1:
				committee_members.append(committee_member)
			else:
				# if we DO have a role to split on:
				(temp_member['name'], temp_member['role']) = committee_member.split(" - ", 1)
				if temp_member['role'] == "Committee Chair":
					committee_chair.append(temp_member['full-string'])
				if temp_member['role'] == "Committee CoChair":
					committee_cochair.append(temp_member['full-string'])
				if temp_member['role'] == "Committee Member":
					committee_members.append(temp_member['full-string'])

		# Sort alphabetically within each role. Most of the names (all of the names?) are in
		# last name comma first name. If they're not, we don't have enough information to plausibly
		# and sensitively identify how to sort by family name with both titles, middle names,
		# and multiple family names in the mix, so we'll sort alphabetically by name as presented
		# in each category and it should *generally* work.
		committee_chair.sort()
		committee_cochair.sort()
		committee_members.sort()
	else:
		return

	committee = committee_chair + committee_cochair + committee_members
	return committee

# Download a file. Return the filename.
def download_file(path, object_id):
	global file_count
	global zip_count
	global sftp_connection

	# parse various parts of the path
	# who knew we'd have a mix of http and https?
	file_path = re.sub("http://d-scholarship.pitt.edu/", "", path)
	file_path = re.sub("https://d-scholarship.pitt.edu/", "", path)
	
	# eprint_id is used for prepending to the file name on save
	eprint_id = re.search("^\d+", file_path).group()
	
	# eprint_id_path is used for building the file path
	eprint_id_path = eprint_id.zfill(6)
	eprint_id_path = eprint_id_path[:2] + "/" + eprint_id_path[2:]
	eprint_id_path = eprint_id_path[:5] + "/" + eprint_id_path[5:]
	
	# file_id is zerofilled in and appended to the file path
	file_id = re.sub("^\d+\/", "", file_path)
	
	# file _name (plus eprint_id) is used for the destination filename
	file_name = re.sub("^\d+\/", "", file_id)

	file_id = re.search("^\d+", file_id).group()
	file_path = "/opt/eprints3/archives/pittir/documents/disk0/00/" + eprint_id_path + "/" + file_id.zfill(2)
	destination_id = eprint_id + "_" + unquote(file_name)
	# not allowed spaces, I guess
	destination_id = re.sub(" ", "_", destination_id)
	# there is one specific ETD that has equals signs in the filenames of the associated files and paramiko chokes on it
	destination_id = re.sub("=", "eq", destination_id)
	print(f"\t\tAssociated File: {file_name} -> {destination_id}")
	log_activity_to_file(f"Associated File: {file_name} -> {destination_id}", LOGFILE_DEFAULT_DETAILS)
	try:
		sftp_connection.get(file_path+"/"+file_name, WORKING_FILES_DIRECTORY+"/"+destination_id)
		return destination_id
	except:
		file_name = unquote(file_name)
		file_name = re.sub("=", "\\=", file_name)
		try: 
			sftp_connection.get(file_path+"/"+file_name, WORKING_FILES_DIRECTORY+"/"+destination_id)
			return destination_id
		except:
			log_activity_to_file(f"Download failed: {file_path+'/'+file_name}", LOGFILE_FAILED_DOWNLOADS)
			print 
			print(f"\t\tFile download failed: {file_name}")
			return
	
			

# class to parse incoming JSON and output JSON
# with this edit, we're going to go clean-slate and work through each field
def parse_object(json_object):
	# regrets
	global categories
	global sftp_connection

	# slightly fewer regrets
	languages = Language_Codes()

	with_errors = False
	parent_tree = []
	new_object = {}
	
	print(f"\tParsing item: {json_object['source_identifier'][0]}")
	log_activity_to_file(f"Parsing item: {json_object['source_identifier'][0]}", LOGFILE_DEFAULT_DETAILS)

	###################################################
	# Admin Fields
	###################################################

	# item field
	# Files go here.
	new_object['item'] = []
	if 'documents/document/files/file/url' in json_object.keys():
		# for each element in the array, download the file
		for url in json_object['documents/document/files/file/url']:
			file_downloaded = download_file(url, json_object['source_identifier'][0])
			if file_downloaded:
				new_object['item'].append(file_downloaded)
	if len(new_object['item']) < 1:
		new_object['item'] = ""

	# quick and dirty fix for the JSON import pulling everything into a list
	for key,value in json_object.items():
		if type(value) is list:
			if len(value) == 1:
				json_object[key] = value[0]

	# source_identifier field AND identifier field
	# source_identifier optional in v3 but required earlier. identifier field optional.
	if 'source_identifier' in json_object.keys():
		new_object['source_identifier'] = json_object['source_identifier']

		if type(new_object['source_identifier']) is list:
			new_object['source_identifier'] = new_object['source_identifier'][0]
		
		# identifier
		# we have two values coming in through the data, but we instead want to use the d-scholarship URL without the interstitial path.
		# This is stored in the source_identifier, and http://d-scholarship.pitt.edu/id/eprint/10172 should become http://d-scholarship.pitt.edu/10172
		# seems easier to do things this way to standardize on the prefix - make sure we're always showing https, which has not been the norm in the data
		new_object['identifier'] = re.sub("https?://d-scholarship.pitt.edu/id/eprint/", "", new_object['source_identifier'])
		new_object['identifier'] = "https://d-scholarship.pitt.edu/" + new_object['identifier']

	# model field
	# Required.
	# always the same
	new_object['model'] = 'Etd'

	# parents field
	# For us, this will be categories.
	temp_categories = []
	if 'parents' in json_object.keys():
		# grab the complete list of parents from the categories tree
		if type(json_object['parents']) is list:
			for parent_id in json_object['parents']:
				temp_categories.append(parent_id)
				if parent_id not in categories.keys():
					print("Error loading categories! Key: "+json_object['parents'])
				else:
					if type(categories[parent_id]['parents']) is list:
						for nested_id in categories[parent_id]['parents']:
							temp_categories.append(nested_id)
					else:
						temp_categories.append(categories[parent_id]['parents'])
		else:
			temp_categories += categories[json_object['parents']]['parents']

	# move temp variable over to object. List and Set nonsense is deduping entries.
	new_object['parents'] = list(set(temp_categories))
	new_object['parents'].sort()

	###################################################
	# Other Fields
	###################################################

	# title field
	if 'title' in json_object.keys():
		new_object['title'] = json_object['title']

	# creator field
	if 'creator' in json_object.keys():
		new_object['creator'] = json_object['creator']

	# keyword field
	# required
	if 'keyword' in json_object.keys():
		new_object['keyword'] = json_object['keyword']
	if 'keyword' not in json_object.keys() or not json_object['keyword']:
		new_object['keyword'] = 'Not Specified'
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_MISSING_KEYWORDS)
		with_errors = True

	# rights field
	# required
	if 'rights' in json_object.keys():
		new_object['rights'] = json_object['rights']
	if 'rights' not in json_object.keys() or not json_object['rights']:
		new_object['rights'] = 'Not Specified'
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: rights", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: rights", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: rights", LOGFILE_MISSING_RIGHTS)
		with_errors = True

	# license field
	if 'license' in json_object.keys():
		new_object['license'] = json_object['license']

	# type field
	if 'type' in json_object.keys():
		new_object['type'] = json_object['type']

	# degree field
	# Required
	if 'degree' in json_object.keys():
		new_object['degree_name'] = json_object.pop('degree')
	if 'degree_name' not in new_object.keys() or not new_object['degree_name']:
		new_object['degree_name'] = "Not Specified"
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_MISSING_DEGREE_NAME)
		with_errors = True

	# level field
	# Required
	if 'level' in json_object.keys():
		new_object['degree_level'] = json_object.pop('level')
	if 'degree_level' not in new_object.keys() or not new_object['degree_level']:
		new_object['degree_level'] = "Not Specified"
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_MISSING_DEGREE_LEVEL)
		with_errors = True

	# discipline field
	if 'discipline' in json_object.keys():
		if type(json_object['discipline']) is list:
			# this should always only be one item
			if not categories[json_object['discipline'][0]]['breadcrumbed_name']:
				log_activity_to_file("Object \""+json_object['discipline']+"\" does not have a match in our categories", LOGFILE_DEFAULT_ERROR)
			else:
				new_object['discipline'] = categories[json_object['discipline'][0]]['breadcrumbed_name']
		else:
			if not categories[json_object['discipline']]['breadcrumbed_name']:
				log_activity_to_file("Object \""+json_object['discipline']+"\" does not have a match in our categories", LOGFILE_DEFAULT_ERROR)
			else:
				new_object['discipline'] = categories[json_object['discipline']]['breadcrumbed_name']

	# grantor field
	if 'grantor' in json_object.keys():
		new_object['grantor'] = json_object['grantor']

	# advisor field
	if 'advisor' in json_object.keys():
		new_object['advisor'] = json_object['advisor']

	# commitee member field
	# running a function to properly order the committee members
	if 'committee_member' in json_object.keys():
		new_object['committee_member'] = parse_committee(json_object['committee_member'])

	# department field
	if 'department' in json_object.keys():
		new_object['department'] = json_object['department']

	# format field
	if 'format' in json_object.keys():
		new_object['format'] = json_object['format']

	# date field
	if 'date' in json_object.keys():
		new_object['date'] = json_object['date']

	# contributor field
	if 'contributor' in json_object.keys():
		new_object['contributor'] = json_object['contributor']

	# description field
	if 'description' in json_object.keys():
		new_object['description'] = json_object['description']

	# publisher field
	if 'publisher' in json_object.keys():
		new_object['publisher'] = json_object['publisher']

	# subject field
	if 'subject' in json_object.keys():
		new_object['subject'] = json_object['subject']

	# language field
	if 'language' in json_object.keys():
		# hax
		if type(json_object['language']) is list:
			new_object['language'] = json_object['language'][0]
		full_language = ""
		full_language = languages.get_language_by_code(json_object['language'])
		if full_language:
			new_object['language'] = full_language

	# relation field
	if 'relation' in json_object.keys():
		new_object['relation'] = json_object['relation']

	# source field
	if 'source' in json_object.keys():
		new_object['source'] = json_object['source']

	# abstract field
	if 'abstract' in json_object.keys():
		new_object['abstract'] = json_object['abstract']

	# admin_note field
	if 'admin_note' in json_object.keys():
		new_object['admin_note'] = json_object['admin_note']

	# ##################################
	#  Embargo Handling
	# ##################################
	
	# Ugh, I had this written out and thought I was debugging it, but it never made it to the server and I lost it.
	# My recollection of what went here:
	
	# if embargo date is after current date, then start processing embargo stuff
	# check if embargo date exists, first
	if 'documents/document/date_embargo' in json_object.keys():

		# reduce embargo date down to a single value
		if type(json_object['documents/document/date_embargo']) is list:
			json_object['documents/document/date_embargo'] = json_object['documents/document/date_embargo'][0]

		# if we have an embargo date in the future
		if datetime.strptime(json_object['documents/document/date_embargo'], '%Y-%m-%d') > datetime.now():
			# we have a possible embargo!
			print(f"\t\tEMBARGO: We have a possible embargo for \"{json_object['source_identifier']}\"")
			
			# assign embargo release date
			new_object['embargo_release_date'] = json_object['documents/document/date_embargo']

			# set embargo - this is an or, not an and, so handling this separately and overwriting is fine
			if 'metadata_visibility' in json_object.keys():
				if type(json_object['metadata_visibility']) is list:
					json_object['metadata_visibility'] = json_object['metadata_visibility'][0]
				# set embargo status
				if json_object['metadata_visibility'] != "show":
					new_object['visibility'] = "embargo"
			if 'full_text_status' in json_object.keys():
				if type(json_object['full_text_status']) is list:
					json_object['full_text_status'] = json_object['full_text_status'][0]
				# set embargo status
				if json_object['full_text_status'] != "show":
					new_object['visibility'] = "embargo"

			# Figure out document security
			if 'documents/document/security' in json_object.keys():
				if type(json_object['documents/document/security']) is list:
					json_object['documents/document/security'] = json_object['documents/document/security'][0]
				if json_object['documents/document/security'] == "public":
					new_object['visibility_during_embargo'] = "open"
				if json_object['documents/document/security'] == "validuser":
					new_object['visibility_during_embargo'] = "authenticated"
				if json_object['documents/document/security'] == "restricted":
					new_object['visibility_during_embargo'] = "restricted"
			
			# set visibility after embargo - if we have an embargo, this is always set
			new_object['visibility_after_embargo'] = "open"
			
			log_activity_to_file(f"{new_object['source_identifier']} - ORIGINAL: embargo_date {json_object['documents/document/date_embargo']} / metadata_visibility {json_object['metadata_visibility']} / full_text_status {json_object['full_text_status']} / security {json_object['documents/document/security']} || NEW EMBARGO INFO - embargo_release_date {new_object['embargo_release_date']} / visibility {new_object['visibility']} / visibility_during_embargo {new_object['visibility_during_embargo']} / visibility_after_embargo {new_object['visibility_after_embargo']}", LOGFILE_EMBARGO)


	# quick and dirty fix for the JSON import pulling everything into a list
	# annoyingly, this needs to be at the end, leaving me to spot-check a bunch of other stuff further above.
	# for the purposes of fixing encoding, I'm fixing it here so I don't iterate over the lists twice
	for key,value in new_object.items():
		if type(value) is list:
			if len(value) == 1:
				log_activity_to_file(f"Fix encoding: {value[0]} -> {fix_encoding(value[0])}")
				new_object[key] = fix_encoding(value[0])
			elif len(value) > 1:
				log_activity_to_file(f"Fix encoding: {value[0]} -> {fix_encoding(stringify_list('|', value))}")
				new_object[key] = fix_encoding(stringify_list("|", value))
		else:
			if value:
				log_activity_to_file(f"Fix encoding: {value} -> {fix_encoding(value)}")
				new_object[key] = fix_encoding(new_object[key])
	if with_errors:
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" was converted, with errors.", LOGFILE_DEFAULT_DETAILS)
	else:
		log_activity_to_file("Object \""+new_object['source_identifier']+"\" was converted.", LOGFILE_DEFAULT_DETAILS)

	return [new_object,with_errors]

# Import the categories we've already processed from the JSON storage.
def load_categories():
	# regretting my life decisions right about now
	global categories
	try:
		with open(DEFINITIONS_DIRECTORY+DIRECTORY_SEPARATOR+CATEGORY_FILENAME, 'r', encoding='utf-8') as f:
			data = json.load(f)
			categories = data
	except FileNotFoundError:
		print("We can't find the file ["+DEFINITIONS_DIRECTORY+DIRECTORY_SEPARATOR+CATEGORY_FILENAME+"]. Please make sure it actually exists and is accessible by the user executing the script!")
	f.close()
	return

# Setting up argument parsing
def parse_arguments():
	parser = argparse.ArgumentParser(description='Parse some JSON.')
	parser.add_argument('infile', metavar='infile', type=str, help='the path to a json-formatted file that will be parsed.')
	parser.add_argument('outfile', metavar='outfile', type=str, help='a filename fragment (no extension) that will store the parsed json. This data may be broken up into multiple files. Output will be formatted as [outfile].[index].json .')
	parser.add_argument('max_size', metavar='maxSize', type=int, help='The maximum number of entries per output file.')

	return parser.parse_args()

# a bit of math to determine how many digits we're going to need for these output files,
# in order to make sure we can zero-pad them and sort them alphabetically.
def zero_pad_size(count, max_size):
	return math.ceil(math.log10(count/max_size))

# clear logs
def clear_logs():
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_DEFAULT, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_DEFAULT_ERROR, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_DEFAULT_DETAILS, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_MISSING_DEGREE_NAME, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_MISSING_DEGREE_LEVEL, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_MISSING_KEYWORDS, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_JSON_CACHE, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_MISSING_RIGHTS, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_FAILED_DOWNLOADS, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_EMBARGO, 'w').close()
	

# reset file system - clear out the working directory
def rebuild_working_dir():
	# Clear out the working directory from the last run, if necessary
	if Path(WORKING_DIRECTORY).exists():
		shutil.rmtree(WORKING_DIRECTORY)
	set_up_directory(WORKING_DIRECTORY)
	set_up_directory(WORKING_FILES_DIRECTORY)


def main():
	args = parse_arguments()

	# Clear out the logs from the last run.
	clear_logs()
	# Clear out the working directory from the last run, if necessary
	rebuild_working_dir()

	# Initialize the batch directory
	set_up_directory(OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+BATCH_NAME)

	log_activity_to_file("Conversion started.")
	log_activity_to_file("Conversion started.", LOGFILE_DEFAULT_DETAILS)

	# data load - json
	# needs to be in latin-1 or the json module crashes on this data
	try:
		with open(INPUT_DIRECTORY+DIRECTORY_SEPARATOR+args.infile, 'r', encoding='latin-1') as f:
			data = json.load(f)
	except FileNotFoundError:
		print("We can't find the file ["+INPUT_DIRECTORY+DIRECTORY_SEPARATOR+args.infile+"]. Please make sure it actually exists and is accessible by the user executing the script!")

	# load in categories from data we've already processed
	load_categories()

	# setup for the primary loop
	file_index = 0
	local_index = 0
	# get the padding size (so we can zero-pad our filenames)
	pad_size = zero_pad_size(len(data), args.max_size)
	# initializing object with empty list
	json_output = []
	new_content = {}
	fieldnames = {}
	files_with_errors = 0
	files_without_errors = 0

	# this is where the work gets done
	# loop through all of the data we've pulled in from the json
	for content in data:
		# increment our count of items for this file
		local_index += 1
		# parse the object as needed
		[new_content,with_errors] = parse_object(content)

		if with_errors:
			files_with_errors += 1
		else:
			files_without_errors += 1

		# adding our content to the list
		json_output.append(new_content)

		# generate field names by looking at the fields we've already generated
		# this may be suboptimal - we know the list of fields for the ETD load
		for fieldname in list(new_content.keys()):
			if(fieldname not in fieldnames):
				fieldnames[fieldname] = fieldname

		# If we've hit our batch size
		if local_index >= args.max_size:
			# debugging
			log_activity_to_file(json.dumps(json_output, indent=4), LOGFILE_JSON_CACHE)
			# end debugging
			
			batch_filename = args.outfile+str(file_index).rjust(pad_size, '0')
			log_activity_to_file("Writing metadata into "+batch_filename, LOGFILE_DEFAULT_DETAILS)
			print(f"\n\tWriting to batch: {batch_filename}\n")

			# Write to an output file. We're using the file_index to build the filename here.
			save_json_to_file(json_output, batch_filename+'.json')
			save_csv_to_file(json_output, batch_filename+'.csv', fieldnames)
			
			# Zip our output file along with our documents, and put it in the working directory
			log_activity_to_file("Creating zip archive: "+WORKING_DIRECTORY+DIRECTORY_SEPARATOR+batch_filename, LOGFILE_DEFAULT_DETAILS)
			shutil.make_archive(batch_filename, 'zip', WORKING_DIRECTORY)

			# move zip file into exports
			batch_filename = batch_filename + ".zip"
			log_activity_to_file("Moving archive to: "+OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+BATCH_NAME+DIRECTORY_SEPARATOR+batch_filename, LOGFILE_DEFAULT_DETAILS)
			shutil.move(batch_filename, OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+BATCH_NAME+DIRECTORY_SEPARATOR+batch_filename)
			
			rebuild_working_dir()
			
			# reset counters for our next file
			json_output = []
			file_index += 1
			local_index = 0

	# write out the last metadata
	if local_index > 0:
		batch_filename = args.outfile+str(file_index).rjust(pad_size, '0')
		log_activity_to_file("Writing metadata into "+batch_filename, LOGFILE_DEFAULT_DETAILS)
		print(f"\n\tWriting to batch: {batch_filename}\n")

		# Write to an output file. We're using the file_index to build the filename here.
		save_json_to_file(json_output, batch_filename+'.json')
		save_csv_to_file(json_output, batch_filename+'.csv', fieldnames)
		
		# Zip our output file along with our documents, and put it in the working directory
		log_activity_to_file("Creating zip archive: "+WORKING_DIRECTORY+DIRECTORY_SEPARATOR+batch_filename, LOGFILE_DEFAULT_DETAILS)
		shutil.make_archive(batch_filename, 'zip', WORKING_DIRECTORY)

		# move zip file into exports
		batch_filename = batch_filename + ".zip"
		log_activity_to_file("Moving archive to: "+OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+BATCH_NAME+DIRECTORY_SEPARATOR+batch_filename, LOGFILE_DEFAULT_DETAILS)
		shutil.move(batch_filename, OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+BATCH_NAME+DIRECTORY_SEPARATOR+batch_filename)
		
		rebuild_working_dir()
		
		# reset counters for our next file
		json_output = []
		file_index += 1
		local_index = 0

	f.close()

	log_activity_to_file("Conversion complete. "+str(files_without_errors)+" objects processed without errors. "+str(files_with_errors)+" objects processed with errors.", LOGFILE_DEFAULT_DETAILS)
	#with open('output.json', 'w', encoding='latin-1') as outputFile:
	#	json.dump(data, outputFile, ensure_ascii=False, indent=4)
	#outputFile.close()


if __name__ == "__main__":  
    main()

# cleanup from opening these globally
sftp_connection.close()
ssh_connection.close()