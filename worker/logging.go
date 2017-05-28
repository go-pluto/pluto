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
		"msg", "failed to run the service",
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
		"method", "Select",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to select successfully")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

func (s *loggingService) Create(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {
	ok := s.Service.Create(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "Create",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to create successfully")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

func (s *loggingService) Delete(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {
	ok := s.Service.Delete(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "Delete",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to delete successfully")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

func (s *loggingService) List(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {
	ok := s.Service.List(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "List",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to list successfully")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

func (s *loggingService) Append(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {
	ok := s.Service.Append(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "Append",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to append successfully")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

func (s *loggingService) Expunge(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {
	ok := s.Service.Expunge(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "Expunge",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to expunge successfully")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}

func (s *loggingService) Store(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {
	ok := s.Service.Store(c, req, syncChan)

	logger := log.With(s.logger,
		"method", "Store",
		"command", req.Command,
		"payload", req.Payload,
	)

	if !ok {
		level.Info(logger).Log("msg", "failed to store successfully")
	} else {
		level.Debug(logger).Log()
	}

	return ok
}
