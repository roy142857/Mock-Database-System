# Mini-Database-Project

This is a Mock Database System Project. In this project, we build a key-value store from scratch. A key-value store (KV-store) is a kind of database system that stores key-value pairs and allows retrieval of a value based on its key.

## Table of Contents

- [Mini-Database-Project](#mini-database-project)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Features](#features)
    - [Open APIs](#open-apis)
    - [Buffer Pool and eviction policy](#buffer-pool-and-eviction-policy)
    - [Memtable and LSM Tree](#memtable-and-lsm-tree)
    - [Bloom filters](#bloom-filters)
  - [Getting Started](#getting-started)
  - [Usage](#usage)
  - [Testing](#testing)
  - [Contact](#contact)

## Introduction

KV-stores are widely used in industry. Note that they have a far simpler API than traditional relational database systems, which expose SQL as an API to the user. There are many applications for which a simple KV API is sufficient. This project allows user to store KV-pairs as entries in a mini-database system. And series of command operations can be perform by user using command line. 

## Features

### Open APIs
We allow users to manipulate the data by executing series of commands such as insert, update, delete and etc.

### Buffer Pool and eviction policy
We implemented buffer pool strategies with the clock algorithm eviction policy to improve query performances. Reduce the amount of I/O cost into the storage.

### Memtable and LSM Tree
We allocated memroy for Memtable to store the data and it will be transform to files when it reaches its maximum capacity. To stores the large files, we use the LSM Tree structure to optimize the query performances. Also, for each file, we construct a binary tree structure for a better performance by searching in this file.

### Bloom filters
We implemented bloom filters for each file to improve the performance for get query API.

## Getting Started

Using `git clone` to clone the repository.
And `git checkout main` to checkout to the main branch.
And then `git pull` to pull the lastest changes.

Now the project is setted up.

In command line, execute `make` to make all the executables

By running the program, execute `./database`, and it will ask user to generate commands.

Now, you can type `help` or `h` to see all the usages to use the program.

## Usage
`open <database name>`: Open a database with associated name.

`put <key> <value>`: Insert a KV-pair into currently opened database.

`get <key>`: Returns the value with associated query key.

`scan <lowerbound> <upperbound>`: Returns a list of KV-pairs with in the scan range includsive.

`update <key> <value>`: Update the value with existing key. If key does not exist, this pair will be inserted.

`delete <key>`: Remove KV-pair with existing key.

`close`: Close currently opened database.

`exit`: Terminate the program and exit.


## Testing

For testing, there are 3 sections of unittests and each sections contains the testing for different functionalities of features. By running the unittest, after compiled the project using `make`.
Execute `./test {number}`, where number is a section number between 1 to 3.