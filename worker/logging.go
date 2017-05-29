package worker

import (
	"net"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/numbleroot/pluto/imap"
)

type loggingService struct {
	logger log.Logger
	Service
}

func NewLoggingService(logger log.Logger, s Service) Service {
	return &loggingService{logger, s}
}

func (s *loggingService) Run() error {

	err := s.Service.Run()

	level.Warn(s.logger).Log(
		"msg", "failed to run worker service",
		"err", err,
	)

	return err
}

func (s *loggingService) HandleConnection(conn net.Conn) error {

	err := s.Service.HandleConnection(conn)

	level.Info(s.logger).Log(
		"msg", "failed to handle connection",
		"err", err,
	)

	return err
}

func (s *loggingService) Select(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.Service.Select(c, req, syncChan)

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

func (s *loggingService) Create(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.Service.Create(c, req, syncChan)

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

func (s *loggingService) Delete(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.Service.Delete(c, req, syncChan)

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

func (s *loggingService) List(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.Service.List(c, req, syncChan)

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

func (s *loggingService) Append(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.Service.Append(c, req, syncChan)

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

func (s *loggingService) Expunge(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.Service.Expunge(c, req, syncChan)

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

func (s *loggingService) Store(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.Service.Store(c, req, syncChan)

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
