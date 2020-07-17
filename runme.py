import subprocess, shlex

def run_script(command):
	args = shlex.split(command)
	return subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0]

if __name__ == '__main__':
	print("YES!", "YEAH!")