# storm-local-multilang

The easiest way to test and develop your multilang spouts and bolts for [Storm][0].  


## Installation

You can download a standalone jar release from the download tab.  


If you want to create the jar yourself you will need Maven (`mvn`) to compile and package the final jar.  
Then, after cloning the repo, you can run the following command : 

    $ mvn package

This will generate a standalone jar in the target directory.

## Usage

The Storm project supports spouts and bolts written in any language via the [Storm Multi-Language Protocol][1]  
To test your multilang topologies you need to define a **topology description file**  

### The topology description file

The file is written in JSON format and is easy to understand.  
Each spout and bolt have a unique name, a command (`php`, `node`, `python` ...) and the path to the script file (relative to the description file).  
You will also need to declare the fields name emitted by your components.  
For every bolt you need to specify the associated grouping(s). For now only shuffle and fields grouping is supported.  
Here is an sample topology description file : 

````json
{
	"name":"testTopology",
	"configuration":{
		"debug":true
	},
	"components":{
		"spouts":{
			"spoutName":{
				"command":"node",
				"path":"spoutScriptPath",
				"fields":["test","test2"]
			}
		},
		"bolts":{
			"boltName":{
				"command":"node",
				"path":"boltScriptPath",
				"fields":["test","test2"],
				"grouping":[
					{
						"type":"shuffle",
						"componentId":"spoutName"
					},
					{
						"type":"fields",
						"componentId":"spoutName",
						"fields":["test", "test2"]
					}
				]
			}
		}
	}
}

````

### Launching your topology

Simply execute the following command :  

	$ java -jar storm-local-multilang-standalone.jar path/to/your/topology/description/file.json

This will launch a Storm `LocalCluster` instance and submit your topology.  


## Licence

Copyright (c) 2012 Michaël Schwartz, EPOKMEDIA <info@epokmedia.fr>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.



[0]: https://github.com/nathanmarz/storm
[1]: https://github.com/nathanmarz/storm/wiki/Multilang-protocol
