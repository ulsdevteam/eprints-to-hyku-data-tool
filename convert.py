import os

try:
	directories = os.listdir("./import")
except FileNotFoundError:
	print("The /import directory needs to exist, and it must have at least one subdirectory.")

[print(i) for i in directories]