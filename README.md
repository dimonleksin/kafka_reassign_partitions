# Kafka Reasign Partitions Golang

## Small about idea

Sometimes i'm needed remove one ore more nodes from cluster of kafka for work, update or some another reason and i can't find no one tool for this. Yes, exist GUI for kafka, and some of them contains mechnism for reassign, but this is not CLI and not always maybe use GUI. This tool work without JWM, you need only one binary file. As long as the tool supports work without authorize or with authorize from scram-sha-256. Pleas, if you needed another methods of authorizing, send me issue in github.

## This tool help you for:

+ Move one or more topic from one to some brokers
    - if for some reason you need moveing one or more topics

+ Move all topics, contains in a broker to some another brokers
    - if you need delete node from cluster

+ Rebalance all topics in cluster
    - if for some reason you moved topics and you cluster in disbalance, you can fix it with --rebalance
    krpg will avally moved all topics in all brokers from you cluster

## How to use this
+ --bootstrap_server [string] for set addres of brokers
    - format: host:port, like 127.0.0.1:9092
+ --topic [string/[]string] for set topics name for move
    - if u send some topics - separator ','
+ --action [string] Set action of u needed (move/return/rebalance)
+ --user [string] set username, if u dont set this arg, used PLAINTEXT
	- if set --user, u need set and --password
+ --password [string] set password for connect to kafka
	- if u set password without --user, this call panic
+ --to [int] set brokers ids for desctination brokers (sep ','). For example 1,2,3
+ --from [int] set source broker id
+ --treads [int] seted number of treads for reassign
+ --version prints the version of this
+ -h or --help for print this help

## Build KRPG

### For linux
    GOOS=linux GOARCH=amd64 GOGCC=false go build -o krpg

### For windows
    GOOS=windows GOARCH=amd64 go build -o krpg.exe