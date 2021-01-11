import subprocess
from os import listdir
from os.path import join,dirname,basename
import csv
import time
import sys

mypath = '/home/olib92/benchmark/queries/'
result_seed = '/home/olib92/benchmark/seeds_2.csv'
classpath = '/home/olib92/apps/xformer/xformer-context.jar:/home/olib92/apps/xformer/lib/*'
main = 'main.dataXFormerDriver'
test_file = '/home/olib92/benchmark/files.csv'
seeds = {}

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

def runXformer_functional(line):
    input_array = line.split(';')
    filename = input_array[0]
    try:
        seed = seeds[filename]
    except KeyError:
        seed = ''
    try:
        output = subprocess.check_output(
            ['java', '-Xms8g', '-Xmx128g', '-cp', classpath, main, join(mypath, filename), input_array[1], input_array[2], 'F', seed], timeout=1800).decode('utf-8')
        print(output)
        list = output.split('\n')
        seed = [i for i in list if i.startswith('#seed:')]
        seed = seed[0].split(':')[1]

        precision = [i for i in list if i.startswith('#Precision:')]
        try:
            precision5 = precision[1].split(':')[1]
            precision10 = precision[2].split(':')[1]
        except IndexError:
            precision5 = precision10 = -999

        recall = [i for i in list if i.startswith('#Recall')]
        try:
            recall5 = recall[1].split(':')[1]
            recall10 = recall[2].split(':')[1]
        except IndexError:
            recall5 = recall10 = -999

        not_found = [i for i in list if i.startswith('#not_found:')]
        try:
            not_found5 = not_found[1].split(':')[1]
            not_found10 = not_found[2].split(':')[1]
        except IndexError:
            not_found5 = not_found10 = -999

        queries = [i for i in list if i.startswith('#query_iterations:')]
        try:
            queries5 = queries[1].split(':')[1]
            queries10 = queries[2].split(':')[1]
        except IndexError:
            queries5 = queries10 = -999

        iterations = [i for i in list if i.startswith('#iterations:')]
        try:
            iterations5 = iterations[1].split(':')[1]
            iterations10 = iterations[2].split(':')[1]
        except IndexError:
            iterations5 = iterations10 = -999

        line5 = [filename, seed, 5, precision5, recall5, queries5, iterations5, not_found5]
        line10 = [filename, seed, 10, precision10, recall10, queries10, iterations10, not_found10]

        list = []
        list.append(line5)
        list.append(line10)
        return list
    except subprocess.CalledProcessError as error:
        raise error
    except subprocess.TimeoutExpired as error:
        raise error

def writeBenchmarkResults(file_content):
    with open('benchmark/results_' + xformer_name + '.csv', 'w', newline='') as outputFile:
        csvwriter = csv.writer(outputFile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(['file', 'seed', 'initial examples', 'precision', 'recall', 'query iterations', 'number iterations', 'not found', 'total values', 'time in sec'])
        for file in file_content:
            filename = file.split(';')[0]
            print('Starting ' + filename + ' ...')
            lines = []
            start_time = time.time()
            try:
                lines = runXformer_functional(file)
                end_time = time.time()
                for line in lines:
                    line.append(file_len(join(mypath, filename)))
                    line.append(end_time-start_time)
            except subprocess.CalledProcessError:
                message = 'An error occured'
                print(message)
                lines.append([filename, '', '5', '','','','',file_len(join(mypath, filename)), file_len(join(mypath, filename)), time.time()-start_time])
                lines.append([filename, '', '10', '','','','',file_len(join(mypath, filename)), file_len(join(mypath, filename)), time.time()-start_time])
            except subprocess.TimeoutExpired:
                message = 'Timed out.'
                print(message)
                lines.append([filename, '', '5', '','','','',file_len(join(mypath, filename)), file_len(join(mypath, filename)), time.time()-start_time])
                lines.append([filename, '', '10', '','','','',file_len(join(mypath, filename)), file_len(join(mypath, filename)), time.time()-start_time])
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
        xformer_name = sys.argv[1].lower()
    except IndexError:
        xformer_name = 'test'
    try:
        use_seed = sys.argv[2].lower() == 'seed'
    except IndexError:
        use_seed = False
    if use_seed is True:
        populate_seeds()
    with open(test_file) as f:
        file_content_temp = f.read().splitlines()
    del(file_content_temp[0])
    file_content = [line for line in file_content_temp if line.split(';')[-2] == 'x']
    writeBenchmarkResults(file_content)
