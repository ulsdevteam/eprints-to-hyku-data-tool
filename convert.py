import argparse, csv, datetime, json, math, os
from pytz import timezone

LOGFILE_DEFAULT = "logs/default.log"
LOGFILE_DEFAULT_ERROR = "logs/error.log"
LOGFILE_DEFAULT_DETAILS = "logs/details.log"
LOGFILE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOGFILE_MISSING_DEGREE_NAME = "logs/missing_degree_name.log"
LOGFILE_MISSING_DEGREE_LEVEL = "logs/missing_degree_level.log"

def log_activity_to_file(logstring="Unknown action.", filename=LOGFILE_DEFAULT):
	eastern = timezone('US/Eastern')
	local_time = datetime.datetime.now(eastern)
	with open(filename, 'a') as output_file:
		output_file.write(local_time.strftime(LOGFILE_DATE_FORMAT)+": "+logstring+"\n")
	output_file.close()

def save_json_to_file(json_object, filename, file_encoding='utf-8'):
	with open(filename, 'w', encoding=file_encoding) as output_file:
		json.dump(json_object, output_file, ensure_ascii=False, indent=4)
	output_file.close()

def save_csv_to_file(json_object, filename, fieldnames, file_encoding='utf-8'):
	with open(filename, 'w', encoding=file_encoding, newline='\r') as output_file:
		writer = csv.DictWriter(output_file, fieldnames=fieldnames, dialect="excel")

		writer.writeheader()
		for row in json_object:
			#for key, value in row.items():
				#print(str(key)+"->"+str(value)+" ("+str(type(value))+")")
			writer.writerow(row)
			#print(json.dumps(row, indent=4))
	output_file.close()

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
	with_errors = False
	# quick and dirty fix for the JSON import pulling everything into a list
	for key,value in json_object.items():
		if type(value) is list:
			if len(value) == 1:
				json_object[key] = value[0]
			elif len(value) > 1:
				json_object[key] = stringify_list("|", value)
	# moving keys
	if 'degree' in json_object.keys():
		json_object['degree_name'] = json_object.pop('degree')
	if 'level' in json_object.keys():
		json_object['degree_level'] = json_object.pop('level')
	# required keys
	if 'degree_name' not in json_object.keys():
		json_object['degree_name'] = "Unknown"
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_name", LOGFILE_DEFAULT_DETAILS)
		with_errors = True
	if 'degree_level' not in json_object.keys():
		json_object['degree_level'] = "Unknown"
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_DEFAULT_ERROR)
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" is missing required field: degree_level", LOGFILE_DEFAULT_DETAILS)
		with_errors = True

	if with_errors:
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" was converted, with errors.", LOGFILE_DEFAULT_DETAILS)
	else:
		log_activity_to_file("Object \""+json_object['source_identifier']+"\" was converted.", LOGFILE_DEFAULT_DETAILS)

	return [json_object,with_errors]

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

	# Clear out the detailed log from the last run.
	open(LOGFILE_DEFAULT_DETAILS, 'w').close()
	log_activity_to_file("Conversion started.")
	log_activity_to_file("Conversion started.", LOGFILE_DEFAULT_DETAILS)

	try:
	#	directories = os.listdir("./import")
		with open(args.infile, 'r', encoding='latin-1') as f:
			data = json.load(f)
	except FileNotFoundError:
		print("We can't find the file ["+args.infile+"]. Please make sure it actually exists and is accessible by the user executing the script!")

	#[print(i) for i in directories]

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
