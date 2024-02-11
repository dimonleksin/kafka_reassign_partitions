package pkg

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

// NumberOfBrockers need equal to replication factor in u cluster
const NumberOfBrockers int = 3

type Settings struct {
	BrokersS *string
	Brokers  []string
	Action   *string
	User     *string
	Passwd   *string
	From     *int
	ToS      *string
	To       []int
	H        *bool
	Help     *bool
	TopicS   *string
	Topics   []string
}

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
		"TopicS",
		"",
		"--TopicS [string] for set TopicS name for move",
	)

	flag.Parse()

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
		return fmt.Errorf("flag --to want contains min %d, have %s", NumberOfBrockers, *s.ToS)
	}
	return nil
}

func (s *Settings) parsingBrokers(separator string) {
	t := strings.Split(*s.BrokersS, separator)
	s.Brokers = append(s.Brokers, t...)
}

func (s *Settings) parsingTopics(separator string) {
	t := strings.Split(*s.TopicS, separator)
	s.Brokers = append(s.Topics, t...)
}

func (s Settings) verifyConf() error {
	log.Printf("start verify configs. Bootstrap server %v", s.Brokers)
	// log.Println(*s.ToS)
	if !*s.H && !*s.Help {
		if len(s.Brokers) > 0 {
			for _, v := range s.Brokers {
				if v == "" {
					return fmt.Errorf("bootstrap servers not find or incorrect. \n\tCurrent value of bootstrap-server %v", s.Brokers)
				}
				if !strings.Contains(v, ":") {
					return fmt.Errorf("u \"--bootstrap-server\" not contains port: %s. -h or --help for print small man", v)
				}
			}
		}
		if len(*s.TopicS) > 0 {
			if *s.From != -1 || *s.Action != "move" {
				return fmt.Errorf("if u set key --TopicS, u can't set key --from or set key --action not aqual 'nove'")
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
	}
	return nil
}

func (s Settings) Conf() (sarama.ClusterAdmin, error) {

	config := sarama.NewConfig()
	if len(*s.User) != 0 {
		config.Net.SASL = struct {
			Enable                   bool
			Mechanism                sarama.SASLMechanism
			Version                  int16
			Handshake                bool
			AuthIdentity             string
			User                     string
			Password                 string
			SCRAMAuthzID             string
			SCRAMClientGeneratorFunc func() sarama.SCRAMClient
			TokenProvider            sarama.AccessTokenProvider
			GSSAPI                   sarama.GSSAPIConfig
		}{Enable: true, Mechanism: sarama.SASLMechanism("SASL-SCRAM-SHA256"), User: *s.User, Password: *s.Passwd}
	}
	config.Version = sarama.V2_8_0_0
	admin, err := sarama.NewClusterAdmin(s.Brokers, config)
	if err != nil {
		return nil, err
	}
	return admin, nil
}
