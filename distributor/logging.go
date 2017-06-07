package distributor

import (
	"net"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/numbleroot/pluto/imap"
)

type loggingService struct {
	logger  log.Logger
	service Service
}

// NewLoggingService wraps a provided existing
// service with the provided logger.
func NewLoggingService(s Service, logger log.Logger) Service {
	return &loggingService{logger, s}
}

// Run wraps this service's Run method with
// added logging capabilities.
func (s *loggingService) Run(listener net.Listener, greeting string) error {

	err := s.service.Run(listener, greeting)

	level.Warn(s.logger).Log(
		"msg", "failed to run distributor service",
		"err", err,
	)

	return err
}

// Capability wraps this service's Capability
// method with added logging capabilities.
func (s *loggingService) Capability(c *imap.Connection, req *imap.Request) bool {

	ok := s.service.Capability(c, req)

	logger := log.With(s.logger,
		"method", "CAPABILITY",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation CAPABILITY correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// Logout wraps this service's Logout method
// with added logging capabilities.
func (s *loggingService) Logout(c *imap.Connection, req *imap.Request) bool {

	ok := s.service.Logout(c, req)

	logger := log.With(s.logger,
		"method", "LOGOUT",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation LOGOUT correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// Login wraps this service's Login method
// with added logging capabilities.
func (s *loggingService) Login(c *imap.Connection, req *imap.Request) bool {

	ok := s.service.Login(c, req)

	logger := log.With(s.logger,
		"method", "LOGIN",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation LOGIN correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// StartTLS wraps this service's StartTLS
// method with added logging capabilities.
func (s *loggingService) StartTLS(c *imap.Connection, req *imap.Request) bool {

	ok := s.service.StartTLS(c, req)

	logger := log.With(s.logger,
		"method", "STARTTLS",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation STARTTLS correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// Proxy wraps this service's Proxy method
// with added logging capabilities.
func (s *loggingService) Proxy(c *imap.Connection, rawReq string) bool {

	ok := s.service.Proxy(c, rawReq)

	logger := log.With(s.logger,
		"method", "Proxy",
		"raw_request", rawReq,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to proxy command to responsible worker")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}
