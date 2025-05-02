# eprints-to-hyku-data-tool
A data transformation tool to manage ETL for our eprints to hyku migration.

The primary executable here is convert-etd.py, which accepts input (from the input directory) and converts it into properly-formatted zip files containing a converted CSV as well as the associated files from what is currently hard-coded to D-Scholarship. These files are intended to match the specs for a Hyku for Consortia import via Bulkrax. The output will also include, for each batch, a JSON file with a JSON counterpart to the CSV. This JSON file has the same data, it's just been encoded as JSON instead of CSV.

The secondary executable here is categories.py which accepts categories as an input and formats them for inclusion into Hyku for Consortia.

Logfiles (and there are a lot of them) will generally go into the logs directory.

## Usage
convert-etd.py [infile] [outfile] [max_size]

infile: name of the input file. Input file is expected to be JSON.

outfile: the filename to be used as output. A batch number will be appended, and this name will be used for both the filename of the zip file and also the filename of the CSV inside the zipped container.

If you choose all_etds, for example, you might have an output batch named all_etds849.zip.

max_size: the maximum size of the batch, measured in source documents, not bytes or files. If you pick 15, the script will partition the total job into batches of 15 documents, each of which may have multiple files associated with them.

Example usage: python3.6 ./convert-etd.py everything_etd_records_2025-04-11.json all_etds 15
 
