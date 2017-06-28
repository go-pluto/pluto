package storage

import (
	"net"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/imap"
	"golang.org/x/net/context"
)

// Structs

type loggingService struct {
	logger  log.Logger
	service Service
}

// Functions

// NewLoggingService wraps a provided existing
// service with the provided logger.
func NewLoggingService(s Service, logger log.Logger) Service {

	return &loggingService{
		logger:  logger,
		service: s,
	}
}

// Init wraps this service's Init method
// with added logging capabilities.
func (s *loggingService) Init(syncSendChans map[string]chan comm.Msg) error {
	return s.service.Init(syncSendChans)
}

// ApplyCRDTUpd wraps this service's ApplyCRDTUpd
// method with added logging capabilities.
func (s *loggingService) ApplyCRDTUpd(applyCRDTUpd chan comm.Msg, doneCRDTUpd chan struct{}) {
	s.service.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)
}

// Serve wraps this service's Serve
// method with added logging capabilities.
func (s *loggingService) Serve(socket net.Listener) error {
	return s.service.Serve(socket)
}

// Prepare wraps this service's Prepare method
// with added logging capabilities.
func (s *loggingService) Prepare(ctx context.Context, clientCtx *imap.Context) (*imap.Confirmation, error) {

	conf, err := s.service.Prepare(ctx, clientCtx)

	logger := log.With(s.logger,
		"method", "Prepare client context",
		"clientID", clientCtx.ClientID,
		"userName", clientCtx.UserName,
		"respWorker", clientCtx.RespWorker,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to process Prepare() (failover) for client connection")
	} else {
		level.Debug(logger).Log()
	}

	return conf, err
}

// Close wraps this service's Close method
// with added logging capabilities.
func (s *loggingService) Close(ctx context.Context, clientCtx *imap.Context) (*imap.Confirmation, error) {

	conf, err := s.service.Close(ctx, clientCtx)

	logger := log.With(s.logger,
		"method", "Close client context",
		"clientID", clientCtx.ClientID,
		"userName", clientCtx.UserName,
		"respWorker", clientCtx.RespWorker,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to process Close() (failover) for client connection")
	} else {
		level.Debug(logger).Log()
	}

	return conf, err
}

// Select wraps this service's Select method
// with added logging capabilities.
func (s *loggingService) Select(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	reply, err := s.service.Select(ctx, comd)

	logger := log.With(s.logger,
		"method", "SELECT",
		"command", comd.Text,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to perform operation SELECT (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return reply, err
}

// Create wraps this service's Create method
// with added logging capabilities.
func (s *loggingService) Create(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	reply, err := s.service.Create(ctx, comd)

	logger := log.With(s.logger,
		"method", "CREATE",
		"command", comd.Text,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to perform operation CREATE (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return reply, err
}

// Delete wraps this service's Delete method
// with added logging capabilities.
func (s *loggingService) Delete(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	reply, err := s.service.Delete(ctx, comd)

	logger := log.With(s.logger,
		"method", "DELETE",
		"command", comd.Text,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to perform operation DELETE (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return reply, err
}

// List wraps this service's List method
// with added logging capabilities.
func (s *loggingService) List(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	reply, err := s.service.List(ctx, comd)

	logger := log.With(s.logger,
		"method", "LIST",
		"command", comd.Text,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to perform operation LIST (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return reply, err
}

// AppendBegin wraps this service's AppendBegin method
// with added logging capabilities.
func (s *loggingService) AppendBegin(ctx context.Context, comd *imap.Command) (*imap.Await, error) {

	await, err := s.service.AppendBegin(ctx, comd)

	logger := log.With(s.logger,
		"method", "APPEND (begin)",
		"command", comd.Text,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to perform begin part of operation APPEND (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return await, err
}

// AppendEnd wraps this service's AppendEnd method
// with added logging capabilities.
func (s *loggingService) AppendEnd(ctx context.Context, mailFile *imap.MailFile) (*imap.Reply, error) {

	reply, err := s.service.AppendEnd(ctx, mailFile)

	logger := log.With(s.logger,
		"method", "APPEND (end)",
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to perform end part of operation APPEND (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return reply, err
}

// AppendAbort wraps this service's AppendAbort
// method with added logging capabilities.
func (s *loggingService) AppendAbort(ctx context.Context, abort *imap.Abort) (*imap.Confirmation, error) {

	conf, err := s.service.AppendAbort(ctx, abort)

	logger := log.With(s.logger,
		"method", "APPEND (abort)",
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to abort APPEND (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return conf, err
}

// Expunge wraps this service's Expunge method
// with added logging capabilities.
func (s *loggingService) Expunge(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	reply, err := s.service.Expunge(ctx, comd)

	logger := log.With(s.logger,
		"method", "EXPUNGE",
		"command", comd.Text,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to perform operation EXPUNGE (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return reply, err
}

// Store wraps this service's Store method
// with added logging capabilities.
func (s *loggingService) Store(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	reply, err := s.service.Store(ctx, comd)

	logger := log.With(s.logger,
		"method", "STORE",
		"command", comd.Text,
	)

	if err != nil {
		level.Info(logger).Log("msg", "failed to perform operation STORE (failover) correctly")
	} else {
		level.Debug(logger).Log()
	}

	return reply, err
}
