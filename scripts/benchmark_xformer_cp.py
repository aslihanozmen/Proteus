import subprocess
from os import listdir
from os.path import isfile, join, isdir, dirname, basename
import csv
import time
import sys

result_seed = '/home/olib92/benchmark/seeds.csv'
#result_seed = r'C:\Users\oli\code\DataTransformationDiscoveryThesis\scripts\results_xformer.csv'
seeds = {}
main = "main.dataXFormerDriver"

def getAllFiles(path):
    if isfile(path) and path.endswith('.csv') or path.endswith('.txt'):
        list = [basename(path)]
        global mypath
        mypath = dirname(path)
        return list
    elif isdir(path):
        onlyfiles = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith('.csv') or f.endswith('.txt')]
        return onlyfiles
    else:
        print('File or directory not found')
        sys.exit(852)


def file_len(fname):
    try:
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    except UnicodeDecodeError:
        with open(fname, encoding="ISO-8859-1") as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    except Exception:
        return -1

def runXformer_functional(file, fromCol='0', toCol='1'):
    try:
        seed = seeds[file]
    except KeyError:
        seed = ''
    try:
        output = subprocess.check_output(
            ['java', '-Xms8g', '-Xmx128g', '-cp', xformer, main, join(mypath, file), fromCol, toCol, 'UF', seed], timeout=600).decode('utf-8')
        list = output.split('\n')
        seed = [i for i in list if i.startswith('#seed:')]
        seed = seed[0].split(':')[1]

        precision = [i for i in list if i.startswith('#Precision:')]
        try:    
            precision3 = precision[0].split(':')[1]
            precision5 = precision[1].split(':')[1]
            precision10 = precision[2].split(':')[1]
        except IndexError:
            precision3 = precision5 = precision10 = -999

        recall = [i for i in list if i.startswith('#Recall')]
        try:
            recall3 = recall[0].split(':')[1]
            recall5 = recall[1].split(':')[1]
            recall10 = recall[2].split(':')[1]
        except IndexError:
            recall3 = recall5 = recall10 = -999

        not_found = [i for i in list if i.startswith('#not_found:')]
        try:
            not_found3 = not_found[0].split(':')[1]
            not_found5 = not_found[1].split(':')[1]
            not_found10 = not_found[2].split(':')[1]
        except IndexError:
            not_found3 = not_found5 = not_found10 = -999

        iterations = [i for i in list if i.startswith('#iterations:')]
        try:
            iterations3 = iterations[0].split(':')[1]
            iterations5 = iterations[1].split(':')[1]
            iterations10 = iterations[2].split(':')[1]
        except IndexError:
            iterations3 = iterations5 = iterations10 = -999

        line3 = [file, seed, 3, precision3, recall3, iterations3, not_found3]
        line5 = [file, seed, 5, precision5, recall5, iterations5, not_found5]
        line10 = [file, seed, 10, precision10, recall10, iterations10, not_found10]

        list = []
        list.append(line3)
        list.append(line5)
        list.append(line10)
        return list
    except subprocess.CalledProcessError as error:
        raise error
    except subprocess.TimeoutExpired as error:
        raise error


def writeBenchmarkResults(files):
    with open('benchmark/results_' + xformer_name + '.csv', 'w', newline='') as outputFile:
        csvwriter = csv.writer(outputFile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(['file', 'seed', 'initial examples', 'precision', 'recall', 'number iterations', 'not found', 'total values', 'time in sec'])
        for file in sorted(files, key=str.lower):
            print('Starting ' + file + ' ...')
            lines = []
            start_time = time.time()
            try:
                if file in ['AthleteDateToEvent.csv', 'AthleteDateToMeetingPlace.csv', 'AuthorYear2Novel.csv', 'LaureateYearToCategory.csv', 'NovelYear2Author.csv', 'TitleYear2Artist.txt', 'videogameReleaseDate2Publisher.csv', 'videogameReleaseDate2Publisher.csv', 'videogameReleaseDate2Publisher.csv']:
                    lines = runXformer_functional(file, '0,1', '2')
                elif file in ['bankCityBranchToSwiftCode11.csv']:
                    lines = runXformer_functional(file, '0,1', '3')
                elif file in ['MovieActorYear2Role.txt']:
                    lines = runXformer_functional(file, '0,1,2', '3')
                else:
                    lines = runXformer_functional(file)

                end_time = time.time()
                for line in lines:
                    line.append(file_len(join(mypath, file)))
                    line.append(end_time-start_time)
            except subprocess.CalledProcessError:
                message = 'An error occured'
                print(message)
                lines.append([file, message, file_len(join(mypath, file)), time.time()-start_time])
            except subprocess.TimeoutExpired:
                message = 'Timed out after 6 minutes.'
                print(message)
                lines.append([file, message, file_len(join(mypath, file)), time.time() - start_time])
            for line in lines:
                print(line)
                csvwriter.writerow(line)

def populate_seeds():
    with open(result_seed) as f:
        content = f.read().splitlines()
    for line in content:
        line_list = line.split(';')
        try:
            int(line_list[1])
            seeds[line_list[0]] = line_list[1]
        except ValueError:
            continue

if __name__ == "__main__":
    try:
        xformer = sys.argv[1].lower()
    except IndexError:
        xformer = '/home/olib92/apps/xformer-abedjan.jar'
    xformer_name = xformer.split('/')[-1].split('.')[0]
    try:
        mypath = sys.argv[2]
    except IndexError:
        mypath = '/home/olib92/benchmark/functional/'
    try:
        use_seed = sys.argv[3].lower() == 'seed'
    except IndexError:
        use_seed = False
    if use_seed is True:
        populate_seeds()
    files = getAllFiles(mypath)
    #print(seeds)
    writeBenchmarkResults(files)
