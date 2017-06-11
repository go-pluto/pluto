package worker

import (
	"net"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/numbleroot/pluto/comm"
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

func (s *loggingService) Init(syncSendChan chan comm.Msg) error {
	return s.service.Init(syncSendChan)
}

func (s *loggingService) ApplyCRDTUpd(applyCRDTUpd chan comm.Msg, doneCRDTUpd chan struct{}) {
	s.service.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)
}

// Run wraps this service's Run method
// with added logging capabilities.
func (s *loggingService) Run() error {

	err := s.service.Run()

	level.Warn(s.logger).Log(
		"msg", "failed to run worker service",
		"err", err,
	)

	return err
}

// HandleConnection wraps this service's HandleConnection
// method with added logging capabilities.
func (s *loggingService) HandleConnection(conn net.Conn) error {

	err := s.service.HandleConnection(conn)

	level.Info(s.logger).Log(
		"msg", "failed to handle connection",
		"err", err,
	)

	return err
}

// Select wraps this service's Select method
// with added logging capabilities.
func (s *loggingService) Select(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.service.Select(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "SELECT",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation SELECT correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// Create wraps this service's Create method
// with added logging capabilities.
func (s *loggingService) Create(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.service.Create(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "CREATE",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation CREATE correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// Delete wraps this service's Delete method
// with added logging capabilities.
func (s *loggingService) Delete(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.service.Delete(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "DELETE",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation DELETE correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// List wraps this service's List method
// with added logging capabilities.
func (s *loggingService) List(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.service.List(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "LIST",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation LIST correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// Append wraps this service's Append method
// with added logging capabilities.
func (s *loggingService) Append(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.service.Append(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "APPEND",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation APPEND correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// Expunge wraps this service's Expunge method
// with added logging capabilities.
func (s *loggingService) Expunge(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.service.Expunge(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "EXPUNGE",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation EXPUNGE correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

// Store wraps this service's Store method
// with added logging capabilities.
func (s *loggingService) Store(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.service.Store(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "STORE",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to perform operation STORE correctly")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}
