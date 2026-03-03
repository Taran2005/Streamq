package broker

type Config struct {
	Port           int
	MaxMessageSize int
}

func DefaultConfig() Config {
	return Config{
		Port:           8080,
		MaxMessageSize: 1_048_576,
	}
}
