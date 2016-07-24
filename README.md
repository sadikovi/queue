# queue
Improved job scheduler for [Apache Spark](https://spark.apache.org)

[![Build Status](https://travis-ci.org/sadikovi/queue.svg?branch=master)](https://travis-ci.org/sadikovi/queue)
[![Coverage Status](https://coveralls.io/repos/github/sadikovi/queue/badge.svg?branch=master)](https://coveralls.io/github/sadikovi/queue?branch=master)

## Requirements
- `python` == 2.7.x
- `pip` latest
- `nodejs` >= 0.10.25
- `npm` >= 1.3.10

You can download official Python distribution from https://www.python.org/downloads/. Note that `pip`
might already be included with Python, otherwise refer to the documentation on how to set it up.
Install `nodejs` and `npm` (for Ubuntu 14.04):
```shell
$ sudo apt-get update
$ sudo apt-get install nodejs npm nodejs-legacy
```

## Build
If you do not have `virtualenv` installed, run this:
```
$ pip install virtualenv
```
Clone repository and set up `virtualenv`:
```shell
$ git clone https://github.com/sadikovi/queue
$ cd queue
$ virtualenv venv
```

Now you can use `bin/python` and `bin/pip` to access python and pip of virtual environment. Run
following commands to install dependencies.
```shell
$ bin/pip install -r requirements.txt --upgrade --target lib/
$ npm install
```

The very first time it is recommended to run `npm run publish`. This will set up folders, compile
and check scripts, run tests and prepare all files in `static` directory that is served when
server is up.

That is it! Now you can launch server using `bin/python setup.py queue_server` with specific
arguments. After that, when you modify scripts, you can just run individual tasks defined
`package.json`, e.g. run `npm run compile_coffee` to just recompile CoffeeScript in static folder.
When you modify Python sources server restarts automatically.

## Test
### Run linters
Run `bin/pylint` from project directory, you should provide `--python` flag for python binaries you
want to use.
```shell
# For example, use bin/python for 'virtualenv'
bin/pylint --python=bin/python
```

You can also verify CoffeeScript files by running this command.
```
$ npm run lint
```

### Run tests
Run `bin/coverage` from project directory, you should also specify `--python` flag to provide link
to your python binaries. This will run Python tests and print overall coverage.
```shell
# For example, use bin/python for 'virtualenv'
$ bin/coverage --python=bin/python
```

Run CoffeeScript tests.
```shell
$ npm run test
```
