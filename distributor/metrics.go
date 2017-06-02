package distributor

import (
	"net"

	"github.com/go-kit/kit/metrics"
	"github.com/numbleroot/pluto/imap"
)

type metricsService struct {
	service Service
	logins  metrics.Counter
	logouts metrics.Counter
}

func NewMetricsService(s Service, logins metrics.Counter, logouts metrics.Counter) Service {
	return &metricsService{
		service: s,
		logins:  logins,
		logouts: logouts,
	}
}

func (s *metricsService) Run(listener net.Listener, greeting string) error {
	return s.service.Run(listener, greeting)
}

func (s *metricsService) Capability(c *imap.Connection, req *imap.Request) bool {
	return s.service.Capability(c, req)
}

func (s *metricsService) Logout(c *imap.Connection, req *imap.Request) bool {

	ok := s.service.Logout(c, req)

	if ok {
		s.logouts.Add(1)
	}

	return ok
}

func (s *metricsService) Login(c *imap.Connection, req *imap.Request) bool {

	ok := s.service.Login(c, req)

	if ok {
		s.logins.Add(1)
	}

	return ok
}

func (s *metricsService) StartTLS(c *imap.Connection, req *imap.Request) bool {
	return s.service.StartTLS(c, req)
}

func (s *metricsService) Proxy(c *imap.Connection, rawReq string) bool {
	return s.service.Proxy(c, rawReq)
}
