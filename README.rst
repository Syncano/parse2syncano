Syncano Parse migration tool
============================

This tool will help you to move your data from Parse to Syncano.

Installation
------------

pip install parse2syncano


Usage
-----

Currently supports only transfer the data. Tool takes the parse schemas and transform it to the Syncano classes. 
Next step is to move all of the data between parse and Syncano. The last step is rebuilding the relations between
objects.


Configuration
-------------

parse2syncano configure

Will run the configuration that will ask you for the following variables:

* PARSE_MASTER_KEY: the master key of your PARSE 
* PARSE_APPLICATION_ID: the application ID of the application that you want to transfer;
* SYNCANO_ADMIN_API_KEY: the Syncano admin api key;
* SYNCANO_INSTANCE_NAME: the Syncano instance name to which the transfer will be made;
* SYNCANO_APIROOT: will not show as it has a default equal to: https://api.syncano.io/

parse2syncano configure command will take following paramters:

* -c (--curent) which will display the current configuration;
* -f (--force) which allow to override the previously set configuration; 

The configuration will be stored in your home directory in .syncano file under the P2S section. 
It's used to call the Parse API and Syncano API as well.

Run transfer
------------
 
parse2syncano sync

This command will run the synchronization process between parse and Syncano. Sit comfortable in your chair and read
the output.

Tips & Troubleshooting
----------------------

1. Tool currently do not support checking if some object is already present in the Syncano instance, so if when sync 
is run twice the end results is that data is doubled. To avoid such cases - simply remove your instance 
in Syncano dashboard.

2. The process can be quite slow - it's because of the throttling on both sides: Parse and Syncano 
on free accounts (which is the bottom boundary for scripts);

3. If you encounter any problems, have some improvements proposal or just wanna talk, 
please write: sebastian.opalczynski@syncano.com
