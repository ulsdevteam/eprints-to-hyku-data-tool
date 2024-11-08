import argparse, csv, datetime, json, math, os, sys
from pytz import timezone
from pathlib import Path
import paramiko # ssh connection
from getpass import getpass # password input

# Well, this started out pretty simple and now I look at it and want to refactor it.
# Apologies for the lack of class structure!

DIRECTORY_SEPARATOR = "/"
INPUT_DIRECTORY = "import"
OUTPUT_DIRECTORY = "output"
WORKING_DIRECTORY = "working"
DEFINITIONS_DIRECTORY = "definitions"
CATEGORY_FILENAME = "categories.json"
LANGUAGE_CODE_TABLE_FILENAME = "languages.json"

BATCH_DATE_FORMAT = "%Y-%m-%d %H-%M-%S"
EASTERN_TIMEZONE = timezone('US/Eastern')
BATCH_START_TIME = datetime.datetime.now(EASTERN_TIMEZONE)
BATCH_NAME = BATCH_START_TIME.strftime(BATCH_DATE_FORMAT)

LOGFILE_DIRECTORY = "logs"
LOGFILE_DEFAULT = "default.log"
LOGFILE_DEFAULT_ERROR = "error.log"
LOGFILE_DEFAULT_DETAILS = "details.log"
LOGFILE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOGFILE_MISSING_DEGREE_NAME = "missing_degree_name.log"
LOGFILE_MISSING_DEGREE_LEVEL = "missing_degree_level.log"
LOGFILE_MISSING_KEYWORDS = "missing_keywords.log"

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
sftp_connection = self.ssh_connection.open_sftp()
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
			print("We can't find the file ["+DEFINITIONS_DIRECTORY+DIRECTORY_SEPARATOR+LANGUAGE_CODE_TABLE_FILENAME+"]. Please make sure it actually exists and is accessible by the user executing the script!")
		f.close()

		for language_code in self.language_table_code_first.keys():
			self.language_table_language_first[self.language_table_code_first[language_code]] = language_code


	def get_language_by_code(self, code):
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
		error_to_terminal("Error setting up directory.")
	return

def log_activity_to_file(logstring="Unknown action.", filename=LOGFILE_DEFAULT):
	local_time = datetime.datetime.now(EASTERN_TIMEZONE)
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
	dirname = OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+BATCH_NAME
	set_up_directory(dirname)
	try:
		with open(dirname+DIRECTORY_SEPARATOR+filename, 'w', encoding=file_encoding, newline='\r') as output_file:
			writer = csv.DictWriter(output_file, fieldnames=fieldnames, dialect="excel")

			writer.writeheader()
			for row in json_object:
				#for key, value in row.items():
					#print(str(key)+"->"+str(value)+" ("+str(type(value))+")")
				writer.writerow(row)
				#print(json.dumps(row, indent=4))
		output_file.close()
	except:
		error_to_terminal("Error writing CSV to file.\nOutput file: "+dirname+DIRECTORY_SEPARATOR+filename)

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

# Download a file
def download_file(path, object_id):
	global sftp_connection
	# parse object_id
	# parse path
	file_path = re.sub("http://d-scholarship.pitt.edu/", "", path)
	eprint_id = re.search("^\d+", file_path)
	eprint_id_path = eprint_id.zfill(6)
	eprint_id_path = eprint_id_path[:2] + "/" + eprint_id_path[:2]
	eprint_id_path = eprint_id_path[:5] + "/" + eprint_id_path[:5]
	file_id = re.sub("^\d+/", "", eprint_id)
	file_id = re.search("^\d+", file_id)
	file_name = re.sub("^\d+/", "", eprint_id)
	file_path = "/opt/eprints3/archives/pittir/documents/disk0/00" + eprint_id.zfill(6) + "/" + file_id

	destination_id = eprint_id + "_" + file_name	
	
	sftp_connection.get(file_path, WORKING_DIRECTORY+destination_ID)


# class to parse incoming JSON and output JSON
def parse_object(json_object):
	# regrets
	global categories
	global sftp_connection

	# slightly fewer regrets
	languages = Language_Codes()

	with_errors = False
	parent_tree = []

	# quick and dirty fix for the JSON import pulling everything into a list
	for key,value in json_object.items():
		if type(value) is list:
			if len(value) == 1:
				json_object[key] = value[0]

	# moving keys
	if 'degree' in json_object.keys():
		json_object['degree_name'] = json_object.pop('degree')
	if 'level' in json_object.keys():
		json_object['degree_level'] = json_object.pop('level')

	# categories
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
	json_object['parents'] = list(set(temp_categories))
	json_object['parents'].sort()

	# disciplines
	if 'discipline' in json_object.keys():
		if type(json_object['discipline']) is list:
			# this should always only be one item
			if not categories[json_object['discipline'][0]]['breadcrumbed_name']:
				log_activity_to_file("Object \""+json_object['discipline']+"\" does not have a match in our categories", LOGFILE_DEFAULT_ERROR)
			else:
				json_object['discipline'] = categories[json_object['discipline'][0]]['breadcrumbed_name']
		else:
			if not categories[json_object['discipline']]['breadcrumbed_name']:
				log_activity_to_file("Object \""+json_object['discipline']+"\" does not have a match in our categories", LOGFILE_DEFAULT_ERROR)
			else:
				json_object['discipline'] = categories[json_object['discipline']]['breadcrumbed_name']

	# Languages
	if 'language' in json_object.keys():
		# hax
		if type(json_object['language']) is list:
			json_object['language'] = json_object['language'][0]
		full_language = ""
		full_language = languages.get_language_by_code(json_object['language'])
		if full_language:
			json_object['language'] = full_language

	# Files
	if 'documents/document/files/file/url' in json_object.keys():
		# for each element in the array, download the file
		for url in json_object['documents/document/files/file/url']:
			download_file(url, json_object['source_identifier'])

	# required keys
	if 'degree_name' not in json_object.keys() or not json_object['degree_name']:
		json_object['degree_name'] = "Not Specified"
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_MISSING_DEGREE_NAME)
		with_errors = True
	if 'degree_level' not in json_object.keys() or not json_object['degree_level']:
		json_object['degree_level'] = "Not Specified"
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_MISSING_DEGREE_LEVEL)
		with_errors = True
	if 'keyword' not in json_object.keys() or not json_object['keyword']:
		json_object['keyword'] = 'Not Specified'
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_MISSING_KEYWORDS)
		with_errors = True

	# other cleanup
	if 'committee_member' in json_object.keys():
#		temp_debug_value = parse_committee(json_object['committee_member'])
		json_object['committee_member'] = parse_committee(json_object['committee_member'])
#		print("Committee:\n"+str(json_object['committee_member']))


	# quick and dirty fix for the JSON import pulling everything into a list
	# annoyingly, this needs to be at the end, leaving me to spot-check a bunch of other stuff further above.
	for key,value in json_object.items():
		if type(value) is list:
			if len(value) == 1:
				json_object[key] = value[0]
			elif len(value) > 1:
				json_object[key] = stringify_list("|", value)

	if with_errors:
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" was converted, with errors.", LOGFILE_DEFAULT_DETAILS)
	else:
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" was converted.", LOGFILE_DEFAULT_DETAILS)

	return [json_object,with_errors]

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

def main():
	args = parse_arguments()

	# Clear out the logs from the last run.
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_DEFAULT, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_DEFAULT_ERROR, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_DEFAULT_DETAILS, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_MISSING_DEGREE_NAME, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_MISSING_DEGREE_LEVEL, 'w').close()
	open(LOGFILE_DIRECTORY+DIRECTORY_SEPARATOR+LOGFILE_MISSING_KEYWORDS, 'w').close()

	log_activity_to_file("Conversion started.")
	log_activity_to_file("Conversion started.", LOGFILE_DEFAULT_DETAILS)

	try:
	#	directories = os.listdir("./import")
		with open(INPUT_DIRECTORY+DIRECTORY_SEPARATOR+args.infile, 'r', encoding='latin-1') as f:
			data = json.load(f)
	except FileNotFoundError:
		print("We can't find the file ["+INPUT_DIRECTORY+DIRECTORY_SEPARATOR+args.infile+"]. Please make sure it actually exists and is accessible by the user executing the script!")

	load_categories()

#	print("Categories:\n\n"+json.dumps(categories))

	file_index = 0
	local_index = 1
	# get the padding size (so we can zero-pad our filenames)
	pad_size = zero_pad_size(len(data), args.max_size)
	# initializing object with empty list
	json_output = []
	new_content = {}
	fieldnames = {}
	files_with_errors = 0
	files_without_errors = 0

	#with open(args.outfile+str(file_index).rjust(pad_size, '0')+'.json', mode='w', encoding='latin-1') as output_file:
	#	json.dump([], output_file)

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
		#print(json.dumps(content, indent=4))

		# generate field names
		for fieldname in list(content.keys()):
			if(fieldname not in fieldnames):
				fieldnames[fieldname] = fieldname

		if local_index >= args.max_size:
			# Write to an output file. We're using the file_index to build the filename here.
			save_json_to_file(json_output, args.outfile+str(file_index).rjust(pad_size, '0')+'.json')
			save_csv_to_file(json_output, args.outfile+str(file_index).rjust(pad_size, '0')+'.csv', fieldnames)
			#print(json.dumps(json_output, indent=4))
			# reset for our next file
			log_activity_to_file("Writing objects to files starting with "+args.outfile+str(file_index).rjust(pad_size, '0'), LOGFILE_DEFAULT_DETAILS)
			json_output = []
			file_index += 1
			local_index = 0

	if local_index > 0:
		# Write to an output file. We're using the file_index to build the filename here.
		save_json_to_file(json_output, args.outfile+str(file_index).rjust(pad_size, '0')+'.json')
		save_csv_to_file(json_output, args.outfile+str(file_index).rjust(pad_size, '0')+'.csv', fieldnames)
		#print(json.dumps(json_output, indent=4))
		# reset for our next file
		log_activity_to_file("Writing objects to files starting with "+args.outfile+str(file_index).rjust(pad_size, '0'), LOGFILE_DEFAULT_DETAILS)
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
sftp_connection.close_connection()
ssh_connection.close_connection()