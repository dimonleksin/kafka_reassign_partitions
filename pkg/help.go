package pkg

import "fmt"

func PrintHelp() {
	textHelp := "--bootstrap_server [string] for set addres of brokers\n" +
		"\t format: host:port, like 127.0.0.1:9092\n\n" +
		"--topic [string] for set topic name for move\n\n" +
		"--action [string] Set action of u needed (move/return/rebalance)\n\n" +
		"--user [string] set username, if u dont set this arg, used PLAINTEXT\n" +
		"\tif set --user, u need set and --password\n\n" +
		"--password [string] set password for connect to kafka\n" +
		"\tif u set password without --user, this call panic\n\n" +
		"--to [int] set brokers ids for desctination brokers (sep ','). For example 1,2,3" +
		"--from [int] set source broker id" +
		"-h or --help for print this help"
	fmt.Println(textHelp)
}
