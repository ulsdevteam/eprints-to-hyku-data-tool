import argparse, csv, json
from pathlib import Path

CATEGORY_FILENAME = "categories.json"
DIRECTORY_SEPARATOR = "/"
INPUT_DIRECTORY = "import"
OUTPUT_DIRECTORY = "definitions"

BREADCRUMB_SEPARATOR = " > "

class Category_Definitions:
	# our workhorse object here. Should be an object.
	categories = {}

	def __init__(self, mode, filename):
		if mode == "regenerate":
			filename = INPUT_DIRECTORY+DIRECTORY_SEPARATOR+filename
			self.import_raw_categories_from_csv(filename)
			self.build_complete_trees_for_categories()
			self.save_categories_to_json()
		elif mode == "load":
			self.import_from_json()

	# Mostly here for debug.
	def print_categories(self):
		print(json.dumps(self.categories, indent=4))

	# Import the categories from a CSV and store them, unmodified, in self.categories.
	#
	# This doesn't do the work of processing them, traversing them, etc. For that,
	# 	we need to run self.build_complete_trees_for_categories.
	def import_raw_categories_from_csv(self, filename):
		temp_object = {}
		with open(filename) as csvfile:
			my_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in my_reader:
				# a rough "skip the header row" rule
				if row[0] == "source_identifier":
					continue
				temp_object = {}
				temp_object['identifier'] = row[0].strip()
				temp_object['model'] = row[1].strip()
				temp_object['raw_parents'] = row[2].strip()
				temp_object['title'] = row[3].strip()
				temp_object['description'] = row[4].strip()
				self.categories[row[0]] = temp_object

		csvfile.close()

	# traverses each tree and builds complete parent lists and breadcrumbed names.
	def build_complete_trees_for_categories(self):
		temp_parent_id = ""
		for category_id in list(self.categories.keys()):
			# create an empty list to hold parents
			self.categories[category_id]['parents'] = []
			# initialize breadcrumb
			self.categories[category_id]['breadcrumbed_name'] = self.categories[category_id]['title']
			#initialize temp_parent_id
			temp_parent_id = self.categories[category_id]['raw_parents']
			while temp_parent_id:
				# We don't want to include these.
				if temp_parent_id == "divisions" or temp_parent_id == "centers":
					break
				# this doesn't protect against all circular references, but it does protect against self-references
				if temp_parent_id == self.categories[temp_parent_id]['raw_parents']:
					print("Recursive error. Parent ID "+temp_parent_id+" has itself as a parent: "+self.categories[temp_parent_id]['raw_parents'])
					break
				# save the parent to the list of parents
				self.categories[category_id]['parents'].append(temp_parent_id)
				# update breadcrumb
				self.categories[category_id]['breadcrumbed_name'] = self.categories[temp_parent_id]['title'] + BREADCRUMB_SEPARATOR + self.categories[category_id]['breadcrumbed_name']
				# iterate upwards
				temp_parent_id = self.categories[temp_parent_id]['raw_parents']

	# Import the categories we've already processed from the JSON storage.
	def import_from_json(self):
		try:
			with open(OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+CATEGORY_FILENAME, 'r', encoding='utf-8') as f:
				data = json.load(f)
		except FileNotFoundError:
			print("We can't find the file ["+OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+CATEGORY_FILENAME+"]. Please make sure it actually exists and is accessible by the user executing the script!")
		self.categories = data

	# Export the categories once we've built them to a JSON file for ease of future loading.
	def save_categories_to_json(self):
		filename = OUTPUT_DIRECTORY+DIRECTORY_SEPARATOR+CATEGORY_FILENAME
		#set_up_directory(dirname)
		try:
			with open(filename, 'w', encoding="utf-8") as output_file:
				json.dump(self.categories, output_file, ensure_ascii=False, indent=4)
			output_file.close()
		except:
			error_to_terminal("Error writing JSON to file.\nOutput file: "+filename)


# Setting up argument parsing
def parse_arguments():
	parser = argparse.ArgumentParser(description='Parse categories from CSV for the eprints-to-hyku migration.')
	parser.add_argument('mode', metavar='mode', type=str, help='The mode. You can either use "load" to load in the saved categories, or "regenerate" to rebuild categories from a file.')
	parser.add_argument('-i', '--infile', metavar='infile', type=str, required=False, help='the path to a json-formatted file that will be parsed. This is required if the mode is "regenerate".')

	return parser.parse_args()

def main():
	args = parse_arguments()
	if args.mode == "regenerate" and not args.infile:
		print("Error: if you are regenerating the categories, you must provide an input file as the second argument.")
		sys.exit(1)

	d_scholarship_cats = Category_Definitions(args.mode, args.infile)

if __name__ == "__main__":  
    main()
