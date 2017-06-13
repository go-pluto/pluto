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
// service with the supplied logger.
func NewLoggingService(s Service, logger log.Logger) Service {

	return &loggingService{
		logger:  logger,
		service: s,
	}
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

// ProxySelect wraps this service's ProxySelect
// method with added logging capabilities.
func (s *loggingService) ProxySelect(c *imap.Connection, rawReq string) bool {

	ok := s.service.ProxySelect(c, rawReq)

	logger := log.With(s.logger,
		"method", "ProxySelect",
		"raw_request", rawReq,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to proxy SELECT command to responsible worker or storage")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// ProxyCreate wraps this service's ProxyCreate
// method with added logging capabilities.
func (s *loggingService) ProxyCreate(c *imap.Connection, rawReq string) bool {

	ok := s.service.ProxyCreate(c, rawReq)

	logger := log.With(s.logger,
		"method", "ProxyCreate",
		"raw_request", rawReq,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to proxy CREATE command to responsible worker or storage")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// ProxyDelete wraps this service's ProxyDelete
// method with added logging capabilities.
func (s *loggingService) ProxyDelete(c *imap.Connection, rawReq string) bool {

	ok := s.service.ProxyDelete(c, rawReq)

	logger := log.With(s.logger,
		"method", "ProxyDelete",
		"raw_request", rawReq,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to proxy DELETE command to responsible worker or storage")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// ProxyList wraps this service's ProxyList
// method with added logging capabilities.
func (s *loggingService) ProxyList(c *imap.Connection, rawReq string) bool {

	ok := s.service.ProxyList(c, rawReq)

	logger := log.With(s.logger,
		"method", "ProxyList",
		"raw_request", rawReq,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to proxy LIST command to responsible worker or storage")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// ProxyAppend wraps this service's ProxyAppend
// method with added logging capabilities.
func (s *loggingService) ProxyAppend(c *imap.Connection, rawReq string) bool {

	ok := s.service.ProxyAppend(c, rawReq)

	logger := log.With(s.logger,
		"method", "ProxyAppend",
		"raw_request", rawReq,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to proxy APPEND command to responsible worker or storage")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// ProxyExpunge wraps this service's ProxyExpunge
// method with added logging capabilities.
func (s *loggingService) ProxyExpunge(c *imap.Connection, rawReq string) bool {

	ok := s.service.ProxyExpunge(c, rawReq)

	logger := log.With(s.logger,
		"method", "ProxyExpunge",
		"raw_request", rawReq,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to proxy EXPUNGE command to responsible worker or storage")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// ProxyStore wraps this service's ProxyStore
// method with added logging capabilities.
func (s *loggingService) ProxyStore(c *imap.Connection, rawReq string) bool {

	ok := s.service.ProxyStore(c, rawReq)

	logger := log.With(s.logger,
		"method", "ProxyStore",
		"raw_request", rawReq,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to proxy STORE command to responsible worker or storage")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}
