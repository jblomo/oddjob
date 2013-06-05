# oddjob

This project gathers useful JVM classes for the mrjob hadoop streaming
framework.  Use the uberjar built by this project as an arguement to
hadoop-streaming's -libjars option to use the classes below.

See also:

https://github.com/Yelp/mrjob  
https://github.com/klbostee/feathers  

## Classes

### oddjob.MultipleTextOutputFormatByKey - Writes to the directories specified by the key
The key of your job output will be used as the file path.  Both the key and the
value will be written to the resulting tab delimited text files.

eg rows:  
filename1	{"some values": "other JSON"}  
otherfile	{"other values": "more JSON"}  

in [outputdir]/filename1/part-00000 will be written filename1	{"some values": "other JSON"}  
in [outputdir]/otherfile/part-00000 will be written otherfile	{"other values": "more JSON"}  

### oddjob.MultipleValueOutputFormat - Writes to the directories specified by the key, and only writes the value
The key of your job output will be used as the file path.  Only the value will
be written to the resulting files.

eg rows:  
filename1	{"some values": "other JSON"}  
otherfile	{"other values": "more JSON"}  

in [outputdir]/filename1/part-00000 will be written {"some values": "other JSON"}  
in [outputdir]/otherfile/part-00000 will be written {"other values": "more JSON"}  

### oddjob.MultipleLeafValueOutputFormat - Writes to the *file* specified by the key, and only writes the value
WARNING: advanced usage only. You probably want MultipleValueOutputFormat
instead.  This class allows the developer to specify the entire path of the
output file, including the "leaf" file (usually 'part-00XXX'). This means the
job /must/ output unique keys per process.  For example, you may use this format
if you guaruntee each key is unique per reducer.

eg rows:  
full/filename	{"some values": "other JSON"}  
otherpath/filename	{"other values": "more JSON"}  

in [outputdir]/full/filename will be written {"some values": "other JSON"}  
in [outputdir]/otherpath/filename will be written {"other values": "more JSON"}  

### oddjob.MultipleCSVOutputFormat - Writes to the directories specified by the first element in a row
The output key of your job must be a comma separated row, fields optionally
enclosed by double quotes.  The first element will be used as the subdirectory,
and the written row will not include that first element.

eg rows:  
"even",16,4  
"odd",25,5  

in [outputdir]/even/part-00000 will be written 16,4  
in [outputdir]/odd/part-00000 will be written 25,5  

### oddjob.MultipleJSONOutputFormat - Writes to the directories specified by the first element in the key
The output key of your job must be a JSON formatted array.  The first element
will be used as the subdirectory, and the second element will be used for key
written to the file.

eg rows:  
[17, "realkey"]	{"some values": "other JSON"}  
[22, "other realkey"]	{"other values": "more JSON"}  

in [outputdir]/17/part-00000 will be written "realkey"	{"some values": "other JSON"}  
in [outputdir]/22/part-00000 will be written "other realkey"	{"other values": "more JSON"}  

### oddjob.ManifestTextInputFormat - Manifest files are inputs with a list of paths to use as the real input.
Paths may be directories, globs, or files and will be expanded appropriately.
Unlike most InputFormats, this class will silently ignore missing and unmatched
paths in the manifest file.

Manifest files are useful for getting around the limit imposed by Amazon EMR on
length of the input paths.

## Usage

    ../lein uberjar
    python examples/mr_wc.py --verbose -r emr  --hadoop-version 0.20  --bootstrap-file oddjob-*-standalone.jar  --bootstrap-cmd 'cp oddjob-*-standalone.jar /home/hadoop/' --hadoop-arg -libjars --hadoop-arg oddjob-*-standalone.jar input.txt

This version of oddjob is designed to be used as a -libjar argument with hadoop 0.20.
-libjar arguments must be available to the hadoop script when you run the job, so you can use an
inline bootstrap-cmd as shown, set it up in your config, or have an AMI with it already installed.

## License

Copyright 2010-2012 Yelp

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
