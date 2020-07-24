from os import mkdir
from pipeline_config import get_folder_names

def setup_dir():
	for folder in get_folder_names().values(): mkdir(folder)

if __name__ == '__main__':
	setup_dir()