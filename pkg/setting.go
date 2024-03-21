package pkg

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

// NumberOfBrockers need equal to replication factor in u cluster
const NumberOfBrockers int = 3

func (s *Settings) GetSettings() error {
	var (
		err error
	)

	const sep string = ","

	s.BrokersS = flag.String(
		"bootstrap-server",
		"",
		"--bootstrap-server [string] Bootstrap server of kafka cluster\nfor example 127.0.0.1:9094",
	)

	s.Action = flag.String(
		"action",
		"rebalance",
		"--action [string] Set action of u needed (move/return/rebalance)",
	)

	s.User = flag.String(
		"user",
		"",
		"--user",
	)

	s.Passwd = flag.String(
		"passwd",
		"",
		"--password need contains u password for acces to cluster",
	)

	s.From = flag.Int(
		"from",
		-1,
		"--from to set numbers of broker to reasign partitions",
	)

	s.ToS = flag.String(
		"to",
		"",
		"--to []int, separator ','. To set number of broker to reasign partitions",
	)

	s.H = flag.Bool(
		"h",
		false,
		"-h/--help for print help",
	)

	s.Help = flag.Bool(
		"help",
		false,
		"-h/--help for print help",
	)

	s.TopicS = flag.String(
		"topic",
		"",
		"--topic [string] for set topic name for move",
	)

	s.Treads = flag.Int(
		"treads",
		1,
		"--treads: number of treads. Default: 1",
	)
	s.KafkaApiVersion = flag.String(
		"api-version",
		"2.7.0",
		"--api-version seted version of brokers",
	)

	s.Version = flag.Bool(
		"version",
		false,
		"--version for print current version of krpg",
	)

	flag.Parse()

	if *s.Version {
		printVersion()
	}

	if !*s.H && !*s.Help {
		if *s.Action == "move" {
			err = s.parsingTo(sep)
			if err != nil {
				return err
			}
		}
		if len(*s.TopicS) > 0 {
			s.parsingTopics(sep)
		}
		s.getKafkaVersion()
		s.parsingBrokers(sep)
		err = s.verifyConf()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Settings) parsingTo(separator string) error {
	if strings.Contains(*s.ToS, separator) {
		for _, v := range strings.Split(*s.ToS, separator) {
			v_int, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.To = append(s.To, v_int)
		}

	} else {
		return fmt.Errorf("flag --to want contains min %d, have %s. -h/--help for more information", NumberOfBrockers, *s.ToS)
	}
	return nil
}

func (s *Settings) getKafkaVersion() {
	var (
		err error
	)
	s.KafkaApiVersionFormated, err = sarama.ParseKafkaVersion(*s.KafkaApiVersion)
	if err != nil {
		fmt.Printf("Error parsing broker api version: %v.\n\tSupported version: %v", err, sarama.SupportedVersions)
		panic("")
	}
}

func (s *Settings) parsingBrokers(separator string) {
	t := strings.Split(*s.BrokersS, separator)
	s.Brokers = append(s.Brokers, t...)
}

func (s *Settings) parsingTopics(separator string) {
	t := strings.Split(*s.TopicS, separator)
	s.Topics = append(s.Topics, t...)
}

func (s Settings) verifyConf() error {
	fmt.Printf("start verify configs. Bootstrap server %v\n", s.Brokers)
	// fmt.Println(*s.ToS)
	if !*s.H && !*s.Help {
		if len(s.Brokers) > 0 {
			for _, v := range s.Brokers {
				if v == "" {
					return fmt.Errorf("bootstrap servers not find or incorrect. \n\tCurrent value of bootstrap-server %v", s.Brokers)
				}
				if !strings.Contains(v, ":") {
					return fmt.Errorf("you \"--bootstrap-server\" not contains port: %s. -h or --help for print small man", v)
				}
			}
		}
		if len(*s.TopicS) > 0 {
			if *s.From != -1 || *s.Action != "move" {
				return fmt.Errorf("if you set key --TopicS, u can't set key --from or set key --action not aqual 'nove'")
			}
		}
		if *s.From != -1 || len(*s.TopicS) > 0 {
			if len(s.To) < NumberOfBrockers {
				return fmt.Errorf("flag --to want contains min %d, have %d", NumberOfBrockers, len(s.To))
			}
		}
		if len(*s.User) > 0 {
			if len(*s.Passwd) == 0 {
				return errors.New("password is not set. -h or --help for more details")
			}
		}
		if len(*s.Passwd) > 0 {
			if len(*s.User) == 0 {
				return errors.New("username is not set. -h or --help for more details")
			}
		}
		if *s.Treads < 1 {
			return fmt.Errorf("number of treads invalid (%d<1) min number of treads: 1", *s.Treads)
		}
	}
	return nil
}

func (s Settings) Conf() (sarama.ClusterAdmin, error) {

	config := sarama.NewConfig()
	if len(*s.User) != 0 {

		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		config.Net.SASL.User = *s.User
		config.Net.SASL.Password = *s.Passwd
		config.Net.SASL.Enable = true
	}

	config.Version = s.KafkaApiVersionFormated
	admin, err := sarama.NewClusterAdmin(s.Brokers, config)
	if err != nil {
		return nil, err
	}
	return admin, nil
}
