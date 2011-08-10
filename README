# oddjob

This project gathers useful JVM classes for the mrjob hadoop streaming
framework.  Use the jar built in this class in place of the hadoop-streaming jar
and use provided classes to change things like the input and output formats.

See also:

https://github.com/Yelp/mrjob
https://github.com/klbostee/feathers

## Classes

### oddjob.MultipleCSVOutputFormat - Writes to the files specified by the first element in a row.
The output key of your job must be a comma seperated row, fields optionally
enclosed by double quotes.  The first element will be used as the subdirectory,
and the written row will not include that first element.

eg rows:
"even",16,4
"odd",25,5

in [outputdir]/even/part-00000 will be written 16,4
in [outputdir]/odd/part-00000 will be written 25,5


### oddjob.MultipleJSONOutputFormat - Writes to the files specified by the first element in the key.
The output key of your job must be a JSON formatted array.  The first element
will be used as the subdirectory, and the second element will be used for key
written to the file.

eg rows:
[17, "realkey"]	{"some values": "other JSON"}
[22, "other realkey"]	{"other values": "more JSON"}

in [outputdir]/17/part-00000 will be written "realkey"	{"some values": "other JSON"}
in [outputdir]/22/part-00000 will be written "other realkey"	{"other values": "more JSON"}

### oddjob.MultipleKeyOutputFormat - Writes to the files specified by the key, and only writes the value
The key of your job output will be used as the file path.  Only the value will
be written to the resulting files.

eg rows:
filename1	{"some values": "other JSON"}
otherfile	{"other values": "more JSON"}

in [outputdir]/filename1/part-00000 will be written {"some values": "other JSON"}
in [outputdir]/otherfile/part-00000 will be written {"other values": "more JSON"}

## Hadoop Versions
This version is compatible with Hadoop 0.20.  The git tag 'hadoop-0.18' contains
a version of oddjob compatible with Hadoop 0.18.  The Usage sections differ
between versions, so please read both if you are switching between versions.

## Usage

../lein uberjar
python your_mrjob.py --hadoop-arg -libjars --hadoop-arg oddjob-*-standalone.jar --outputformat oddjob.MultipleJSONOutputFormat [other options]

## License

Copyright 2010 Yelp

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
