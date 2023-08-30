# C FFI Test

A simple c program to read a delta table and print it out.

This uses the glib based capi that arrow provides.

# Building

The included makefile assumes you have arrow and arrow-glib installed. See [Installing Apache Arrow](https://arrow.apache.org/install/)

It also uses `pkg-config` to determine what libs/flags to pass to gcc

To build simply do `make`

## Running
To run you can `make run`
