package settings

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/dimonleksin/kafka_reasign_partition/internal/stuff"
	"github.com/dimonleksin/kafka_reasign_partition/internal/stuff/event"
	"gopkg.in/yaml.v3"
)

func (s *Settings) GetSettings() error {
	var (
		err         error
		path_to_cfg string
	)

	const sep string = ","

	s.MoveSetting.BackupVersion = *flag.Int(
		"backup-version",
		0,
		"--backup-version [int] unnaccessary version of backup for restore (use if --action restore)",
	)

	s.BootstrapSettings.BrokersS = flag.String(
		"bootstrap-server",
		"",
		"--bootstrap-server [string] Bootstrap server of kafka cluster\nfor example 127.0.0.1:9094",
	)

	action := flag.String(
		"action",
		"rebalance",
		"--action [string] Set action of u needed (move/restore/rebalance)",
	)

	s.BootstrapSettings.Security.User = flag.String(
		"user",
		"",
		"--user",
	)

	s.BootstrapSettings.Security.Passwd = flag.String(
		"password",
		"",
		"--password need contains u password for acces to cluster",
	)
	s.BootstrapSettings.Security.Mechanism = flag.String(
		"mechanism",
		"scram-sha-256",
		"--mechanism need contains mechanism for auth in cluster. Supported scram-sha-256 or scram-sha-512",
	)
	s.BootstrapSettings.Security.Tls.CAPath = flag.String(
		"ca",
		"",
		"--ca need contains path to ca cert",
	)
	s.BootstrapSettings.Security.Tls.CertPath = flag.String(
		"cert",
		"",
		"--cert need contains path to cert",
	)
	s.BootstrapSettings.Security.Tls.KeyPath = flag.String(
		"key",
		"",
		"--key need contains path to TLS key",
	)
	s.BootstrapSettings.Security.Tls.UseTLS = flag.Bool(
		"tls",
		false,
		"--tls enebling tls",
	)
	s.MoveSetting.From = flag.Int(
		"from",
		-1,
		"--from to set numbers of broker to reasign partitions",
	)

	s.MoveSetting.ToS = flag.String(
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

	s.MoveSetting.TopicS = flag.String(
		"topic",
		"",
		"--topic [string] for set topic name for move",
	)

	s.MoveSetting.Treads = flag.Int(
		"treads",
		1,
		"--treads: number of treads. Default: 1",
	)
	s.MoveSetting.Sync = *flag.Bool(
		"async",
		false,
		"sync/async work with topic. If true - krpg not wait when all replicas for partition moved in desired state",
	)
	s.BootstrapSettings.KafkaApiVersion = flag.String(
		"api-version",
		"2.7.0",
		"--api-version seted version of brokers",
	)

	s.Version = flag.Bool(
		"version",
		false,
		"--version for print current version of krpg",
	)

	s.Verbose = *flag.Bool(
		"v",
		false,
		"-v for verbose output",
	)

	path_to_cfg = *flag.String("file", "./krpg.yaml", "--file for set path to settings file. Default - ./krpg.yaml")

	flag.Parse()
	s.MoveSetting.Action = event.GetEvent(*action)

	if *s.Version {
		stuff.PrintVersion()
	}

	if !*s.H && !*s.Help {
		if canReadCert(path_to_cfg) {
			s.readYamlSettings(path_to_cfg)
		}
		if s.MoveSetting.Action == event.MOVE {
			err = s.parsingTo(sep)
			if err != nil {
				return err
			}
		}
		if len(*s.MoveSetting.TopicS) > 0 {
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
	if strings.Contains(*s.MoveSetting.ToS, separator) {
		for _, v := range strings.Split(*s.MoveSetting.ToS, separator) {
			v_int, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("error convert string with value from --to key to int, err: %v", err)
			}
			s.MoveSetting.To = append(s.MoveSetting.To, v_int)
		}

	} else {
		v_int, err := strconv.Atoi(*s.MoveSetting.ToS)
		if err != nil {
			return fmt.Errorf("error convert string with value from --to key to int, err: %v", err)
		}
		s.MoveSetting.To = append(s.MoveSetting.To, v_int)

	}
	return nil
}

func (s *Settings) getKafkaVersion() {
	var err error
	s.KafkaApiVersionFormated, err = sarama.ParseKafkaVersion(*s.BootstrapSettings.KafkaApiVersion)
	if err != nil {
		fmt.Printf("Error parsing broker api version: %v.\n\tSupported version: %v", err, sarama.SupportedVersions)
		panic("")
	}
}

func (s *Settings) parsingBrokers(separator string) {
	t := strings.Split(*s.BootstrapSettings.BrokersS, separator)
	s.BootstrapSettings.Brokers = append(s.BootstrapSettings.Brokers, t...)
}

func (s *Settings) parsingTopics(separator string) {
	t := strings.Split(*s.MoveSetting.TopicS, separator)
	s.MoveSetting.Topics = append(s.MoveSetting.Topics, t...)
}

func (s Settings) checkBrokers() error {
	for _, v := range s.BootstrapSettings.Brokers {
		if v == "" {
			return fmt.Errorf("bootstrap servers not find or incorrect. \n\tCurrent value of bootstrap-server %v", s.BootstrapSettings.Brokers)
		}
		if !strings.Contains(v, ":") {
			return fmt.Errorf("you \"--bootstrap-server\" not contains port: %s. -h or --help for print small man", v)
		}
	}
	return nil
}

func (s Settings) verifyConf() error {
	fmt.Printf("start verify configs. Bootstrap server %v\n", s.BootstrapSettings.Brokers)
	if !*s.H && !*s.Help {
		if len(s.BootstrapSettings.Brokers) == 0 {
			return fmt.Errorf("--bootstrap-server is empty")
		}
		if err := s.checkBrokers(); err != nil {
			return err
		}
		if len(*s.MoveSetting.TopicS) > 0 {
			if *s.MoveSetting.From != -1 || s.MoveSetting.Action != event.MOVE {
				return fmt.Errorf("if you set key --TopicS, u can't set key --from or set key --action not aqual 'nove'")
			}
		}
		if *s.MoveSetting.From != -1 || len(*s.MoveSetting.TopicS) > 0 {
			if len(s.MoveSetting.To) == 0 {
				return fmt.Errorf("flag --to want contains min 1 broker, have %d", len(s.MoveSetting.To))
			}
		}
		if len(*s.BootstrapSettings.Security.User) > 0 {
			if len(*s.BootstrapSettings.Security.Passwd) == 0 {
				return errors.New("password is not set. -h or --help for more details")
			}
		}
		if len(*s.BootstrapSettings.Security.Passwd) > 0 {
			if len(*s.BootstrapSettings.Security.User) == 0 {
				return errors.New("username is not set. -h or --help for more details")
			}
		}
		if *s.MoveSetting.Treads < 1 {
			return fmt.Errorf("number of treads invalid (%d<1) min number of treads: 1", *s.MoveSetting.Treads)
		}
		tlss := s.BootstrapSettings.Security.Tls
		if *tlss.UseTLS {
			if len(*tlss.CAPath) != 0 && !canReadCert(*tlss.CAPath) {
				return fmt.Errorf("can`t read CA file")
			}
			if len(*tlss.CertPath) != 0 && !canReadCert(*tlss.CertPath) {
				return fmt.Errorf("can`t read cert file")
			}
			if len(*tlss.CertPath) != 0 && (len(*tlss.KeyPath) == 0 || !canReadCert(*tlss.KeyPath)) {
				return fmt.Errorf("can`t read cert file or path to cert file not set")
			}
		}

	}
	return nil
}

// Check, can read cert and return false if file not found or error reading
func canReadCert(path string) bool {
	if len(path) > 0 {
		_, err := os.ReadFile(path)
		return err == nil
	}
	return false
}

func (s Settings) Conf() (sarama.ClusterAdmin, error) {
	var err error
	config := sarama.NewConfig()
	if len(*s.BootstrapSettings.Security.User) != 0 {
		config, err = s.updateConfigWithSecurity(config)
		if err != nil {
			return nil, err
		}
	}
	if *s.BootstrapSettings.Security.Tls.UseTLS {
		config, err = s.updateConfigWithTLS(config)
		if err != nil {
			return nil, err
		}
	}
	config.Version = s.KafkaApiVersionFormated
	admin, err := sarama.NewClusterAdmin(s.BootstrapSettings.Brokers, config)
	if err != nil {
		return nil, err
	}
	return admin, nil
}

func (s Settings) updateConfigWithSecurity(config *sarama.Config) (*sarama.Config, error) {
	switch *s.BootstrapSettings.Security.Mechanism {
	case "scram-sha-256":
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	case "scram-sha-512":
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	default:
		return nil, fmt.Errorf("supported scram-sha-256 or scram-sha-512 sasl mechanisms")
	}
	config.Net.SASL.User = *s.BootstrapSettings.Security.User
	config.Net.SASL.Password = *s.BootstrapSettings.Security.Passwd
	config.Net.SASL.Enable = true
	return config, nil
}

func (s Settings) updateConfigWithTLS(config *sarama.Config) (*sarama.Config, error) {
	certs := s.BootstrapSettings.Security.Tls
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		InsecureSkipVerify: false,
	}
	config.Net.TLS.Config.RootCAs = x509.NewCertPool()
	if len(*certs.CAPath) != 0 {
		cert, _ := os.ReadFile(*certs.CAPath)
		config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(cert)
	} else {
		config.Net.TLS.Config.RootCAs = nil
	}
	if len(*certs.CertPath) != 0 {
		key_pair, err := tls.LoadX509KeyPair(*certs.CertPath, *certs.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("error parsing key pair of client: %v", err)
		}
		config.Net.TLS.Config.Certificates = []tls.Certificate{key_pair}
	}
	return config, nil
}

func (s *Settings) readYamlSettings(path string) {
	settings_file, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("settings file not exist")
		return
	}
	err = yaml.Unmarshal(settings_file, s)
	if err != nil {
		panic(fmt.Sprintf("cannot read settings yaml. Err: %v", err))
	}
}
