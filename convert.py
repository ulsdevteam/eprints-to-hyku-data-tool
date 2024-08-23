import argparse, csv, datetime, json, math, os, sys
from pytz import timezone
from pathlib import Path

# Well, this started out pretty simple and now I look at it and want to refactor it.
# Apologies for the lack of class structure!

DIRECTORY_SEPARATOR = "/"
INPUT_DIRECTORY = "import"
OUTPUT_DIRECTORY = "output"
DEFINITIONS_DIRECTORY = "definitions"
CATEGORY_FILENAME = "categories.json"

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

categories = {}

class logger:
	pass

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

# class to parse incoming JSON and output JSON
def parse_object(json_object):
	# regrets
	global categories

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
		#print("Parents: "+json_object['parents'])
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

	# required keys
	if 'degree_name' not in json_object.keys() or not json_object['degree_name']:
		json_object['degree_name'] = "Unknown Degree Name"
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_MISSING_DEGREE_NAME)
		with_errors = True
	if 'degree_level' not in json_object.keys() or not json_object['degree_level']:
		json_object['degree_level'] = "Unknown Degree Level"
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_MISSING_DEGREE_LEVEL)
		with_errors = True
	if 'keyword' not in json_object.keys() or not json_object['keyword']:
		json_object['keyword'] = 'Missing Keywords'
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_DEFAULT_DETAILS)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: keyword", LOGFILE_MISSING_KEYWORDS)
		with_errors = True

	# quick and dirty fix for the JSON import pulling everything into a list
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
