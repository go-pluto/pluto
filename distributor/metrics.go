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

// NewMetricsService wraps a provided existing
// service with defined Prometheus metrics.
func NewMetricsService(s Service, logins metrics.Counter, logouts metrics.Counter) Service {

	return &metricsService{
		service: s,
		logins:  logins,
		logouts: logouts,
	}
}

// Run wraps this service's Run method with
// a metrics exposer.
func (s *metricsService) Run(listener net.Listener, greeting string) error {
	return s.service.Run(listener, greeting)
}

// Capability wraps this service's Capability
// method with a metrics exposer.
func (s *metricsService) Capability(c *imap.Connection, req *imap.Request) bool {
	return s.service.Capability(c, req)
}

// Logout wraps this service's Logout
// method with a metrics exposer.
func (s *metricsService) Logout(c *imap.Connection, req *imap.Request) bool {

	ok := s.service.Logout(c, req)

	if ok {
		s.logouts.Add(1)
	}

	return ok
}

// Login wraps this service's Login
// method with a metrics exposer.
func (s *metricsService) Login(c *imap.Connection, req *imap.Request) bool {

	ok := s.service.Login(c, req)

	if ok {
		s.logins.Add(1)
	}

	return ok
}

// StartTLS wraps this service's StartTLS
// method with a metrics exposer.
func (s *metricsService) StartTLS(c *imap.Connection, req *imap.Request) bool {
	return s.service.StartTLS(c, req)
}

// ProxySelect wraps this service's ProxySelect
// method with a metrics exposer.
func (s *metricsService) ProxySelect(c *imap.Connection, rawReq string) bool {
	return s.service.ProxySelect(c, rawReq)
}

// ProxyCreate wraps this service's ProxyCreate
// method with a metrics exposer.
func (s *metricsService) ProxyCreate(c *imap.Connection, rawReq string) bool {
	return s.service.ProxyCreate(c, rawReq)
}

// ProxyDelete wraps this service's ProxyDelete
// method with a metrics exposer.
func (s *metricsService) ProxyDelete(c *imap.Connection, rawReq string) bool {
	return s.service.ProxyDelete(c, rawReq)
}

// ProxyList wraps this service's ProxyList
// method with a metrics exposer.
func (s *metricsService) ProxyList(c *imap.Connection, rawReq string) bool {
	return s.service.ProxyList(c, rawReq)
}

// ProxyAppend wraps this service's ProxyAppend
// method with a metrics exposer.
func (s *metricsService) ProxyAppend(c *imap.Connection, rawReq string) bool {
	return s.service.ProxyAppend(c, rawReq)
}

// ProxyExpunge wraps this service's ProxyExpunge
// method with a metrics exposer.
func (s *metricsService) ProxyExpunge(c *imap.Connection, rawReq string) bool {
	return s.service.ProxyExpunge(c, rawReq)
}

// ProxyStore wraps this service's ProxyStore
// method with a metrics exposer.
func (s *metricsService) ProxyStore(c *imap.Connection, rawReq string) bool {
	return s.service.ProxyStore(c, rawReq)
}
