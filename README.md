# Map-Reduce

Репозиторий включает:
- map_script.cpp - выполняет операцию Map
- reduce_script.cpp - выполняет операцию Reduce
Оба скрипта читаю из stdin и пишут в stdout. Необходимо скомпилировать их на машине, получив исполняемые файлы.
- MapReduce.java - исходных код программы на java. Необходимо скомпилировать javac c:/..filepath../MapReduce.java. Запускается командой
  java -classpath filepath MapReduce с параметрами: имя выпонлняемой операции(map или reduce); путь к соответствующему исполняемому
  скрипту; путь к файлу, из которого будет читать исполняемый скрипт; путь к файлу, в который будет писать испоняемый скрипт; 
  агрументы исполняемого скрипта (опционально).
- text.txt - пример входного файла (формат tsv)
- transitional.txt - файл, получившийся после применения операции Map к text.txt (формат tsv)
- wordcount.txt - файл, получившийся после применения операции Reduce к transition.txt (формат tsv)
