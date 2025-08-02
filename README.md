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

+ Restore state before changes

## Backups
Backup files stored in ~/.krpg_backup.v.[1-10].
Backup files rotate autamated, without settings.
Max numbaer of backups - 10.

## How to use this
+ --bootstrap_server [string] for set addres of brokers
    - format: host:port, like 127.0.0.1:9092
+ --topic [string/[]string] for set topics name for move
    - if u send some topics - separator ','
+ --action [string] Set action of u needed (move/restore/rebalance)
+ --user [string] set username, if u dont set this arg, used PLAINTEXT
	- if set --user, u need set and --password
+ --password [string] set password for connect to kafka
	- if u set password without --user, this call panic
+ --mechanism [string] scram-sha-256 or scram-sha-512
	- defining only if you use auth with loggin and password
+ --tls define if need uth TLS
+ --ca [string] path to CA file
+ --cert [string] path to cert file
    - if you define --cert, dont forget defined --key
+ --key [string] path to key file
+ --to [int] set brokers ids for desctination brokers (sep ','). For example 1,2,3
+ --from [int] set source broker id
+ --treads [int] seted number of treads for reassign
+ --async sync/async work with topics. If you define this key - krpg not wait when all replicas for partition moved in desired state
+ --api-version set version of brokers in format 2.1.0 (default)
+ --file for set path to settings file. Default - ./krpg.yaml
+ --version prints the version of this
+ -h or --help for print this help


## Multi treads

If you set --treads > 1, process of reassign division and running parrallels, one topic(with all self partitions) - one gorutines(tread)

## Build KRPG

### For linux
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o krpg

### For windows
    GOOS=windows GOARCH=amd64 go build -o krpg.exe


### New Release

Befare release dont forget change version in internal/stuff/version.go & .github/workflows/go.yml in release_name and tag_name