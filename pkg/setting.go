package pkg

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

// NumberofBrockers need equal to replication factor in u cluster
const NumberofBrockers int = 3

type Settings struct {
	BrokersS *string
	Brokers  []string
	Action   string
	User     string
	Passwd   string
	From     int
	To       []int8
}

func (s *Settings) GetSettings() error {
	var (
		// 	// tmp_brockers string
		// 	// tmp_to       string
		err error
	)

	const sep string = ","

	s.BrokersS = flag.String(
		"bootstrap-server",
		"",
		"--bootstrap-server [string] Bootstrap server of kafka cluster\nfor example 127.0.0.1:9094",
	)

	// if strings.Contains(tmp_brockers, sep) {
	// 	s.parsingBrokers(tmp_brockers, sep)
	// } else {
	// s.Brokers = []string{tmp_brockers}
	// }

	s.Action = *flag.String(
		"action",
		"rebalance",
		"--action [string] Set action of u needed (move/return/rebalance)",
	)

	s.User = *flag.String(
		"user",
		"",
		"--user",
	)

	s.Passwd = *flag.String(
		"passwd",
		"",
		"--password need contains u password for acces to cluster",
	)

	// s.From = *flag.Int(
	// 	"from",
	// 	-1,
	// 	"--from to set numbers of broker to reasign partitions",
	// )

	// tmp_to = *flag.String(
	// 	"to",
	// 	"",
	// 	"--to []int, separator ','. To set number of broker to reasign partitions",
	// )

	flag.Parse()

	s.parsingBrokers(sep)

	// if err != nil {
	// 	return err
	// }

	err = s.verifyConf()
	if err != nil {
		return err
	}

	return nil
}

func (s *Settings) parsingTo(str, separator string) error {
	if strings.Contains(str, separator) {
		for _, v := range strings.Split(str, separator) {
			v_int, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			s.To = append(s.To, int8(v_int))
		}

	} else {
		return fmt.Errorf("flag --to want contains min %d, have %s", NumberofBrockers, str)
	}
	return nil
}

func (s *Settings) parsingBrokers(separator string) {
	t := strings.Split(*s.BrokersS, separator)
	s.Brokers = append(s.Brokers, t...)
}

func (s Settings) verifyConf() error {
	log.Printf("start verify configs. Bootstrap server %v", s.Brokers)
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
	// if len(s.To) < NumberofBrockers {
	// 	return fmt.Errorf("flag --to want contains min %d, have %d", NumberofBrockers, len(s.To))
	// }

	return nil
}

func (s Settings) Conf() (sarama.ClusterAdmin, error) {

	config := sarama.NewConfig()
	if len(s.User) != 0 {
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
		}{Enable: true, Mechanism: sarama.SASLMechanism("SASL-SCRAM-SHA256"), User: s.User, Password: s.Passwd}
	}
	config.Version = sarama.V2_8_0_0
	admin, err := sarama.NewClusterAdmin(s.Brokers, config)
	if err != nil {
		return nil, err
	}
	return admin, nil
}
