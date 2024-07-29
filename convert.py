import argparse, csv, json, math, os

def save_json_to_file(json_object, filename, file_encoding='utf-8'):
	with open(filename, 'w', encoding=file_encoding) as output_file:
		json.dump(json_object, output_file, ensure_ascii=False, indent=4)
	output_file.close()

def save_csv_to_file(json_object, filename, fieldnames, file_encoding='utf-8'):
	with open(filename, 'w', encoding=file_encoding, newline='\r') as output_file:
		writer = csv.DictWriter(output_file, fieldnames=fieldnames, dialect="excel")

		writer.writeheader()
		for row in json_object:
			for key, value in row.items():
				print(str(key)+"->"+str(value)+" ("+str(type(value))+")")
			writer.writerow(row)
			#print(json.dumps(row, indent=4))
	output_file.close()

# class to parse incoming JSON and output JSON
def parse_object(json_object):
	for key,value in json_object.items():
		if type(value) is list:
			if len(value) == 1:
				json_object[key] = value[0]
	return json_object

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

	#with open(args.outfile+str(file_index).rjust(pad_size, '0')+'.json', mode='w', encoding='latin-1') as output_file:
	#	json.dump([], output_file)

	for content in data:
		# increment our count of items for this file
		local_index += 1
		# parse the object as needed
		new_content = parse_object(content)
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
			json_output = []
			file_index += 1
			local_index = 0

	if local_index > 0:
		# Write to an output file. We're using the file_index to build the filename here.
		save_json_to_file(json_output, args.outfile+str(file_index).rjust(pad_size, '0')+'.json')
		save_csv_to_file(json_output, args.outfile+str(file_index).rjust(pad_size, '0')+'.csv', fieldnames)
		#print(json.dumps(json_output, indent=4))
		# reset for our next file
		json_output = []
		file_index += 1
		local_index = 0

	f.close()

	#with open('output.json', 'w', encoding='latin-1') as outputFile:
	#	json.dump(data, outputFile, ensure_ascii=False, indent=4)
	#outputFile.close()


if __name__ == "__main__":  
    main()
