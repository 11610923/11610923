Documentation
=============

The Batch Monitor monitors the batch at the defined frequency and captures all the open and delayed branches. It will prepare the respective json message and publish it to the Control service queue.

Batch Monitoring Flow
=====================

1. Event Bridge starts the "Batch Monitoring" function at the defined frequency.
2. Process scans the Batching transaction metadata tables.
3. Captures all the Open and Delayed batches beyond the window end date, from transaction tables.
4. Verifies if any open or delayed batches.
	If no End the process.
5. For all the open or delayed bactches, prepare respective JSON message.
6. Publish it to the Control service queue.
7. Ends the process.

Installing Dependent Modules
============================

If you're using a recent version of Debian or Ubuntu Linux, you can
install with the system package manager:

    $ apt-get install python-module`

package name is published through PyPi, so if you can't install it
with the system packager, you can install it with ``easy_install`` or
``pip``. The package name is ``packagename``, and the same package
works on Python 2 and Python 3.

    $ easy_install packagename

    $ pip install packagename
	
To install necessary packages:

	$ pip install uuid
	
	$ pip install unittest
	
	$ pip install psycopg2
	
	$ pip install boto3
	
	$ pip install botocore
	
	$ pip install jsonschema

If all else fails, the license for package allows you to
package the entire library with your application. 

I use Python 2.7 and Python 3.2 to develop package, but it
should work with other recent versions.

