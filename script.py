from random import randint
import sys
import os

if __name__ == "__main__":
	pages_count = int(sys.argv[1])
	pages_popularity = []
	pages_buffer = ""
	os.mkdir(format(pages_count))
	file_name = format(pages_count)+"/urls.txt"
	file_rank_name = format(pages_count)+"/ranks.txt"

	with open(file_name, 'a') as file:     
		for i in range(0, pages_count):
			pages_popularity.append(randint(0, 99))

		for i in range(0, pages_count):
			for j in range(0, pages_count):
				if i == j:
					continue
				if randint(0, 99) < pages_popularity[j]:
					file.write("https://www.example" + str(i+1) + ".org/ " + "https://www.example" + str(j+1) + ".org/" + '\n')
	
	with open(file_rank_name, 'a') as file:
		for i in range(0, pages_count):
			file.write("https://www.example" + str(i+1) + ".org/ " + str(1) + '\n')
