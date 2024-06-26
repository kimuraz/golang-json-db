# Golang JSON DB: Because who needs fancypants :jeans: SQL databases anyway? :rofl:

This glorious bored-in-the-weekend-project, meticulously crafted in Go (although still learning it), throws away all conventional database wisdom and builds a brand new one... entirely out of JSON, rage, tears and duct tape (okay, maybe not duct tape).

## Why JSON and Binary Files?

- Flexibility: JSON lets us store pretty much anything, from your grandma's secret cookie recipe to your cat's sleeping positions (both equally valuable data, of course).
- Speed: Binary files are like ninjas of data storage - fast, silent, and efficient. Perfect for when you need to retrieve your cat memes in a flash.

## But Wait, There's More! :exploding_head:

This bad boy also boasts:

- Indexing: Find your data faster than a hummingbird searching for sugar water (almost).
- CRUD Operations: Create, Read, Update, and Delete - all the verbs your data will ever need.

> So, is this the future of databases? Probably not. But hey, it's a fun ride!

## Getting Started (Because we know you're itching to try it out)

Clone this repo.

Grab a Go (1.22+) compiler (you know the drill).

Look around the code and run what you like!

Not ready yet, so just play around.

## Running sockets server/client :handshake:

Run the server:
```bash
$ make run-server
```

In another terminal session, run the client:

```bash
$ make run-client
```

## Makefile help

```bash
$ make help
```

## CLI Commands

```
NAME:
   gjdb - Golang JSON DB is a fun simple project implementing a JSON-based db from scratch

USAGE:
   gjdb [global options] command [command options] 

COMMANDS:
   server, svr  Server commands, it uses config.json by default
   client, cl   Client commands
   help, h      Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h  show help
```